package dashboard

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/catalog"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/loadgen"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/store"
)

type adminView struct {
	CSRF        string
	Msg         string
	Err         string
	Rules       []limiter.Rule
	Plans       []catalog.Plan
	Settings    catalog.Settings
	ActiveStore string
	Keys        []store.KeyInfo
	Runs        []store.Run
	RunActive   bool
	MaxRPS      int
	MaxSeconds  int
	MaxClients  int
}

func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	v := adminView{
		CSRF: s.auth.csrfToken(), Msg: r.URL.Query().Get("msg"), Err: r.URL.Query().Get("err"),
		RunActive: s.runner.active(), MaxRPS: s.opt.MaxRPS, MaxSeconds: int(s.opt.MaxDuration.Seconds()), MaxClients: s.opt.MaxClients,
	}
	var err error
	if v.Rules, err = s.opt.Store.ListRules(ctx); err == nil {
		if v.Plans, err = s.opt.Store.ListPlans(ctx); err == nil {
			if v.Settings, err = s.opt.Store.Settings(ctx); err == nil {
				if v.Keys, err = s.opt.Store.ListKeys(ctx, 50); err == nil {
					v.Runs, err = s.opt.Store.ListRuns(ctx, 25)
				}
			}
		}
	}
	if err != nil {
		s.opt.Log.Error("admin page", "err", err)
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	v.ActiveStore = v.Settings.Store
	if v.ActiveStore == "" {
		v.ActiveStore = s.opt.DefaultStore
	}
	s.render(w, "admin.html", v)
}

func redirect(w http.ResponseWriter, r *http.Request, msg string, err error) {
	q := url.Values{}
	if err != nil {
		q.Set("err", err.Error())
	} else {
		q.Set("msg", msg)
	}
	http.Redirect(w, r, "/admin?"+q.Encode(), http.StatusSeeOther)
}

func formInt(r *http.Request, name string, lo, hi int) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(r.PostFormValue(name)))
	if err != nil || n < lo || n > hi {
		return 0, errors.New(name + " must be a number between " + strconv.Itoa(lo) + " and " + strconv.Itoa(hi))
	}
	return n, nil
}

func pathID(r *http.Request) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("bad id")
	}
	return id, nil
}

// bump tells all instances about a change: keys=true clears their API key
// caches (revocation), keys=false reloads rules and settings. Failure is
// logged, not fatal: the 60-second full reload and cache TTL still apply it.
func (s *Server) bump(r *http.Request, keys bool) {
	if s.opt.Bumper == nil {
		return
	}
	var err error
	if keys {
		err = s.opt.Bumper.BumpKeys(r.Context())
	} else {
		err = s.opt.Bumper.BumpConfig(r.Context())
	}
	if err != nil {
		s.opt.Log.Warn("version bump failed (Redis down?); instances apply the change within 60s", "err", err)
	}
}

func (s *Server) handleStartLoadTest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	clients, err := formInt(r, "clients", 1, s.opt.MaxClients)
	if err != nil {
		redirect(w, r, "", err)
		return
	}
	rps, err := formInt(r, "rps", 1, s.opt.MaxRPS)
	if err != nil {
		redirect(w, r, "", err)
		return
	}
	secs, err := formInt(r, "duration", 1, int(s.opt.MaxDuration.Seconds()))
	if err != nil {
		redirect(w, r, "", err)
		return
	}
	pattern := r.PostFormValue("pattern")
	if pattern != loadgen.PatternSteady && pattern != loadgen.PatternBoundary {
		redirect(w, r, "", errors.New("pattern must be steady or boundary"))
		return
	}
	if pattern == loadgen.PatternBoundary && rps*clients*2 > s.opt.MaxRPS {
		redirect(w, r, "", errors.New("boundary bursts: burst size × clients × 2 must stay under the request cap"))
		return
	}
	plan, err := s.opt.Store.PlanByName(ctx, r.PostFormValue("plan"))
	if err != nil {
		redirect(w, r, "", errors.New("unknown plan"))
		return
	}
	rules, err := s.opt.Store.ListRules(ctx)
	if err != nil {
		redirect(w, r, "", errors.New("database unavailable"))
		return
	}
	var rule limiter.Rule
	for _, ru := range rules {
		if ru.ID == plan.RuleID {
			rule = ru
		}
	}
	settings, err := s.opt.Store.Settings(ctx)
	if err != nil {
		redirect(w, r, "", errors.New("database unavailable"))
		return
	}
	storeName := settings.Store
	if storeName == "" {
		storeName = s.opt.DefaultStore
	}
	id, err := s.runner.start(ctx, loadRequest{
		Plan: plan.Name, Clients: clients, RPS: rps, Duration: time.Duration(secs) * time.Second, Pattern: pattern,
	}, plan, rule, storeName)
	if err != nil {
		if !errors.Is(err, errBusy) {
			s.opt.Log.Error("start load test", "err", err)
			err = errors.New("could not start the load test")
		}
		redirect(w, r, "", err)
		return
	}
	redirect(w, r, "Load test #"+itoa64(id)+" started. Watch it on the live page.", nil)
}

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	for _, key := range []string{"store", "failure_mode"} {
		v := r.PostFormValue(key)
		if v == "" {
			continue
		}
		if (key == "store" && v != "memory" && v != "redis") || (key == "failure_mode" && !limiter.FailureMode(v).Valid()) {
			redirect(w, r, "", errors.New("invalid value for "+key))
			return
		}
		before, _ := s.opt.Store.Settings(ctx)
		if err := s.opt.Store.SetSetting(ctx, key, v); err != nil {
			s.opt.Log.Error("save setting", "err", err)
			redirect(w, r, "", errors.New("could not save settings"))
			return
		}
		if key == "store" && before.Store != v {
			s.runner.storeSwitched(v)
		}
	}
	s.bump(r, false)
	redirect(w, r, "Settings saved. Instances apply them within about 2 seconds.", nil)
}

func (s *Server) handleUpdateRule(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		redirect(w, r, "", err)
		return
	}
	limit, err := formInt(r, "limit", 1, 1_000_000)
	if err != nil {
		redirect(w, r, "", err)
		return
	}
	windowMs, err := formInt(r, "window_ms", 100, 86_400_000)
	if err != nil {
		redirect(w, r, "", err)
		return
	}
	burst, err := formInt(r, "burst", 0, 1_000_000)
	if err != nil {
		redirect(w, r, "", err)
		return
	}
	rule := limiter.Rule{ID: id, Algorithm: limiter.Algorithm(r.PostFormValue("algorithm")), Limit: limit,
		Window: time.Duration(windowMs) * time.Millisecond, Burst: burst}
	if err := rule.Validate(); err != nil {
		redirect(w, r, "", err)
		return
	}
	if err := s.opt.Store.UpdateRule(r.Context(), rule); err != nil {
		s.opt.Log.Error("update rule", "err", err)
		redirect(w, r, "", errors.New("could not update rule"))
		return
	}
	s.bump(r, false)
	redirect(w, r, "Rule updated.", nil)
}

func (s *Server) handleCreateKey(w http.ResponseWriter, r *http.Request) {
	owner := strings.TrimSpace(r.PostFormValue("owner"))
	if owner == "" || len(owner) > 64 {
		redirect(w, r, "", errors.New("owner must be 1 to 64 characters"))
		return
	}
	plan, err := s.opt.Store.PlanByName(r.Context(), r.PostFormValue("plan"))
	if err != nil {
		redirect(w, r, "", errors.New("unknown plan"))
		return
	}
	raw, err := s.opt.Signer.Generate()
	var info store.KeyInfo
	if err == nil {
		info, err = s.opt.Store.CreateKey(r.Context(), raw, owner, plan.ID, nil)
	}
	if err != nil {
		redirect(w, r, "", errors.New("could not create key"))
		return
	}
	// Rendered directly (not redirected) so the raw key never appears in a
	// URL or browser history. This is the only time it is shown.
	s.render(w, "key_created.html", map[string]any{"Key": raw, "Info": info})
}

func (s *Server) handleRevokeKey(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err == nil {
		err = s.opt.Store.RevokeKey(r.Context(), id)
	}
	if err != nil {
		redirect(w, r, "", errors.New("could not revoke key"))
		return
	}
	s.bump(r, true)
	redirect(w, r, "Key revoked. It stops working within about 2 seconds (60 at most if Redis is down).", nil)
}

func (s *Server) handlePinRun(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		redirect(w, r, "", err)
		return
	}
	note := strings.TrimSpace(r.PostFormValue("note"))
	label := strings.TrimSpace(r.PostFormValue("marker_label"))
	if len(note) > 200 || len(label) > 60 {
		redirect(w, r, "", errors.New("note or marker label too long"))
		return
	}
	var marker *int
	if v := strings.TrimSpace(r.PostFormValue("marker_second")); v != "" {
		n, err := formInt(r, "marker_second", 0, 3600)
		if err != nil {
			redirect(w, r, "", err)
			return
		}
		marker = &n
	} else {
		// keep an automatically recorded marker
		if run, err := s.opt.Store.GetRun(r.Context(), id); err == nil {
			marker = run.MarkerSecond
			if label == "" {
				label = run.MarkerLabel
			}
		}
	}
	if err := s.opt.Store.PinShowcase(r.Context(), id, note, marker, label); err != nil {
		redirect(w, r, "", errors.New("could not pin run (only finished runs can be pinned)"))
		return
	}
	s.invalidateReplay()
	redirect(w, r, "Run #"+itoa64(id)+" is now the showcase replay.", nil)
}

func (s *Server) handleRunDetail(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	run, err := s.opt.Store.GetRun(r.Context(), id)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	secs, err := s.opt.Store.RunSeconds(r.Context(), id)
	if err != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}
	rp := NewReplay(run, secs)
	marker := -1
	if run.MarkerSecond != nil {
		marker = *run.MarkerSecond
	}
	chart := RenderChart(rp.Points, ChartOptions{Upto: -1, MarkerAt: marker, MarkerLabel: run.MarkerLabel,
		XLabelEvery: max(1, len(rp.Points)/6), XLabel: func(i int) string { return itoa(i) + "s" }})

	// per-instance totals
	type instTotal struct {
		Instance                  string
		Allowed, Rejected, Errors int
	}
	totals := map[string]*instTotal{}
	var order []string
	for _, sec := range secs {
		t := totals[sec.Instance]
		if t == nil {
			t = &instTotal{Instance: sec.Instance}
			totals[sec.Instance] = t
			order = append(order, sec.Instance)
		}
		t.Allowed += sec.Allowed
		t.Rejected += sec.Rejected
		t.Errors += sec.Errors
	}
	var list []instTotal
	for _, name := range order {
		list = append(list, *totals[name])
	}
	var allowedPerSec string
	if run.DurationS > 0 {
		allowedPerSec = formatFloat(float64(run.Allowed) / float64(run.DurationS))
	}
	s.render(w, "run.html", map[string]any{"Run": run, "Chart": chart, "Instances": list,
		"AllowedPerSec": allowedPerSec, "CSRF": s.auth.csrfToken()})
}

func itoa(n int) string     { return strconv.Itoa(n) }
func itoa64(n int64) string { return strconv.FormatInt(n, 10) }
func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', 2, 64)
}
