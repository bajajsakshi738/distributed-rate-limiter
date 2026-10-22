// Package dashboard is the web UI: a public, read-only live view of the
// limiter, and admin pages for rules, keys, settings and load tests.
// It uses Go templates and htmx; there is no hand-written JavaScript.
package dashboard

import (
	"bytes"
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/apikey"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/catalog"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static
var staticFS embed.FS

type templateHTML = template.HTML

// Store is the subset of the Postgres store the dashboard uses.
type Store interface {
	ListRules(ctx context.Context) ([]limiter.Rule, error)
	UpdateRule(ctx context.Context, r limiter.Rule) error
	ListPlans(ctx context.Context) ([]catalog.Plan, error)
	PlanByName(ctx context.Context, name string) (catalog.Plan, error)
	Settings(ctx context.Context) (catalog.Settings, error)
	SetSetting(ctx context.Context, key, value string) error
	CreateKey(ctx context.Context, raw, owner string, planID int64, runID *int64) (store.KeyInfo, error)
	CreateRunKeys(ctx context.Context, keys []string, planID, runID int64) error
	ListKeys(ctx context.Context, limit int) ([]store.KeyInfo, error)
	RevokeKey(ctx context.Context, id int64) error
	RevokeRunKeys(ctx context.Context, runID int64) error
	CreateRun(ctx context.Context, r store.Run) (int64, error)
	FinishRun(ctx context.Context, r store.Run, seconds []store.RunSecond) error
	ListRuns(ctx context.Context, limit int) ([]store.Run, error)
	GetRun(ctx context.Context, id int64) (store.Run, error)
	ShowcaseRun(ctx context.Context) (store.Run, error)
	RunSeconds(ctx context.Context, runID int64) ([]store.RunSecond, error)
	PinShowcase(ctx context.Context, runID int64, note string, markerSecond *int, markerLabel string) error
	SetRunMarker(ctx context.Context, runID int64, second int, label string) error
	NoteStoreSwitch(ctx context.Context, runID int64, newStore string) error
}

// Bumper tells the API instances about changes (version markers in Redis).
type Bumper interface {
	BumpConfig(ctx context.Context) error
	BumpKeys(ctx context.Context) error
}

type Options struct {
	Store          Store
	Signer         *apikey.Signer
	DemoKey        string // public demo key used by the "Try it yourself" button; "" hides it
	Bumper         Bumper
	InstanceURLs   []string // internal base URLs, e.g. http://api-1:9090
	LoadTarget     string   // fixed load-test target, e.g. http://nginx/api/data
	AdminUser      string
	AdminPassword  string
	CSRFSecret     []byte
	DefaultStore   string
	MaxRPS         int
	MaxDuration    time.Duration
	MaxClients     int
	Log            *slog.Logger
	DisablePolling bool // tests
}

type Server struct {
	opt    Options
	tmpl   *template.Template
	poller *Poller
	auth   *adminAuth
	runner *runner

	trySlots chan struct{} // bounds concurrent "Try it yourself" requests

	replayMu    sync.Mutex
	replay      *Replay
	replayAt    time.Time
	replayDirty bool

	cacheMu   sync.Mutex
	cacheBody []byte
	cacheAt   time.Time
}

func New(opt Options) (*Server, error) {
	tmpl, err := template.New("").Funcs(template.FuncMap{
		"fmtTime": func(t time.Time) string { return t.Format("02 Jan 15:04") },
		"us": func(p *int) string {
			if p == nil {
				return "–"
			}
			if *p >= 1000 {
				return template.HTMLEscapeString(formatFloat(float64(*p)/1000)) + " ms"
			}
			return template.HTMLEscapeString(itoa(*p)) + " µs"
		},
		"intp": func(p *int) string {
			if p == nil {
				return "–"
			}
			return itoa(*p)
		},
		"ms":    func(d time.Duration) int64 { return d.Milliseconds() },
		"comma": comma,
	}).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}
	s := &Server{
		opt:    opt,
		tmpl:   tmpl,
		poller: NewPoller(opt.InstanceURLs),
		auth:   newAdminAuth(opt.AdminUser, opt.AdminPassword, opt.CSRFSecret),
	}
	s.trySlots = make(chan struct{}, tryMaxInFlight)
	s.runner = &runner{s: s}
	return s, nil
}

// Start launches background polling. Call once.
func (s *Server) Start(ctx context.Context) {
	if !s.opt.DisablePolling {
		go s.poller.Run(ctx, s.runner.observe)
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	static, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", cacheStatic(http.FileServer(http.FS(static)))))
	mux.HandleFunc("GET /{$}", s.handleLivePage)
	mux.HandleFunc("GET /partials/live", s.handleLivePartial)
	mux.HandleFunc("POST /try", s.handleTry)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })

	admin := http.NewServeMux()
	admin.HandleFunc("GET /admin", s.handleAdmin)
	admin.HandleFunc("GET /admin/runs/{id}", s.handleRunDetail)
	admin.HandleFunc("POST /admin/loadtest", s.handleStartLoadTest)
	admin.HandleFunc("POST /admin/settings", s.handleSettings)
	admin.HandleFunc("POST /admin/rules/{id}", s.handleUpdateRule)
	admin.HandleFunc("POST /admin/keys", s.handleCreateKey)
	admin.HandleFunc("POST /admin/keys/{id}/revoke", s.handleRevokeKey)
	admin.HandleFunc("POST /admin/runs/{id}/pin", s.handlePinRun)
	// Every /admin route goes through auth (and CSRF for POSTs) here, in one
	// place, so a new handler can't accidentally be left unprotected.
	mux.Handle("/admin", s.auth.middleware(admin))
	mux.Handle("/admin/", s.auth.middleware(admin))

	return securityHeaders(limitBody(mux))
}

func (s *Server) render(w http.ResponseWriter, name string, data any) {
	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		s.opt.Log.Error("template", "name", name, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(buf.Bytes())
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		// htmx is served from this origin and configured not to inject inline
		// styles or use eval, so no 'unsafe-inline' or 'unsafe-eval' is needed.
		h.Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "+
			"connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'; object-src 'none'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		// "same-origin", not "no-referrer": with no-referrer, browsers send
		// "Origin: null" on form POSTs, and the CSRF check (rightly) rejects a
		// null origin, which broke every admin form. same-origin still sends
		// nothing to other sites.
		h.Set("Referrer-Policy", "same-origin")
		h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		next.ServeHTTP(w, r)
	})
}

func limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
		next.ServeHTTP(w, r)
	})
}

func cacheStatic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		next.ServeHTTP(w, r)
	})
}

// ---- public pages ----

func (s *Server) handleLivePage(w http.ResponseWriter, r *http.Request) {
	data := map[string]any{"Try": s.opt.DemoKey != "", "TryLimit": ""}
	// Describe the demo key's real limit, read from the rules, so the text
	// can't drift from what the button will actually hit.
	if rules, err := s.opt.Store.ListRules(r.Context()); err == nil {
		for _, ru := range rules {
			if ru.Name == "demo" {
				data["TryLimit"] = describeLimit(ru)
			}
		}
	}
	s.render(w, "live.html", data)
}

// describeLimit turns a rule into words, e.g. "3 requests every 10 seconds".
func describeLimit(r limiter.Rule) string {
	unit := "requests"
	if r.Limit == 1 {
		unit = "request"
	}
	switch w := r.Window; {
	case w == time.Second:
		return itoa(r.Limit) + " " + unit + " per second"
	case w%time.Second == 0:
		return itoa(r.Limit) + " " + unit + " every " + itoa(int(w/time.Second)) + " seconds"
	default:
		return itoa(r.Limit) + " " + unit + " every " + w.String()
	}
}

// handleLivePartial is polled by htmx every second. The rendered HTML is
// cached for one second and shared by all viewers, so 1 viewer and 1,000
// viewers cost the server the same.
func (s *Server) handleLivePartial(w http.ResponseWriter, r *http.Request) {
	s.cacheMu.Lock()
	if time.Since(s.cacheAt) >= time.Second || s.cacheBody == nil {
		var buf bytes.Buffer
		if err := s.tmpl.ExecuteTemplate(&buf, "live_partial.html", s.liveView(r.Context(), time.Now())); err != nil {
			s.cacheMu.Unlock()
			s.opt.Log.Error("render live", "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		s.cacheBody, s.cacheAt = buf.Bytes(), time.Now()
	}
	body := s.cacheBody
	s.cacheMu.Unlock()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Write(body)
}

func (s *Server) liveView(ctx context.Context, now time.Time) LiveView {
	insts := s.poller.Snapshot()
	points, rows, recent, hit := buildLive(insts, now)
	v := LiveView{Instances: rows, Recent: recent, CacheHitRate: hit, RunActive: s.runner.active()}
	for _, in := range rows {
		if !in.OK {
			continue
		}
		if v.Store == "" {
			v.Store, v.FailureMode = in.Store, in.FailureMode
		}
		if in.Store == "redis" && in.Breaker != "" && in.Breaker != "closed" {
			v.RedisDown = true
		}
	}
	last := points[len(points)-1]
	v.AllowedNow, v.RejectedNow = last.Allowed, last.Rejected
	for _, p := range points {
		v.Allowed60 += p.Allowed
		v.Rejected60 += p.Rejected
	}

	v.Ratio = "–"
	if rule, clients, started, ok := s.runner.progress(); ok {
		// While a test runs, the boxes count from its start, and "got
		// through" compares that with the most the limit allows in that time.
		// The ratio waits a few seconds, until the burst allowance stops
		// making it jumpy.
		end := lastCompleteSecond(insts, now)
		elapsed := time.Unix(end+1, 0).Sub(started)
		if n := min(liveSeconds, int(end-started.Unix())+1); n > 0 {
			v.SinceStart = true
			v.Allowed60, v.Rejected60 = 0, 0
			for _, p := range points[len(points)-n:] {
				v.Allowed60 += p.Allowed
				v.Rejected60 += p.Rejected
			}
			if most := float64(clients) * rule.MaxAllowed(elapsed); most > 0 && elapsed >= 3*time.Second {
				v.Ratio = formatRatio(float64(v.Allowed60) / most)
			}
		}
	}

	if v.RunActive || hasRecentTraffic(points, 5) {
		v.Mode = "live"
		v.Banner = "Live"
		v.Chart = RenderChart(points, ChartOptions{
			Upto: -1, MarkerAt: -1, XLabelEvery: 10,
			XLabel: func(i int) string { return "-" + itoa(liveSeconds-1-i) + "s" },
		})
		return v
	}

	rp := s.currentReplay(ctx)
	if rp == nil || len(rp.Points) == 0 {
		v.Mode = "idle"
		v.Banner = "Nothing running right now"
		v.Chart = RenderChart(points, ChartOptions{Upto: -1, MarkerAt: -1, XLabelEvery: 10,
			XLabel: func(i int) string { return "-" + itoa(liveSeconds-1-i) + "s" }})
		return v
	}
	frame := rp.Frame(now)
	shown := min(frame+1, len(rp.Points))
	marker := -1
	if rp.Run.MarkerSecond != nil {
		marker = *rp.Run.MarkerSecond
	}
	v.Mode = "replay"
	run := rp.Run
	v.Run = &run
	v.Banner = "Replay of a recorded test (" + run.StartedAt.Format("2 Jan") + "), not live"
	v.ReplayClock = clock(min(frame, len(rp.Points))) + " / " + clock(len(rp.Points))
	v.Chart = RenderChart(rp.Points, ChartOptions{
		Upto: shown, MarkerAt: marker, MarkerLabel: run.MarkerLabel, XLabelEvery: max(1, len(rp.Points)/6),
		XLabel: func(i int) string { return itoa(i) + "s" },
	})
	if shown > 0 {
		p := rp.Points[shown-1]
		v.AllowedNow, v.RejectedNow = p.Allowed, p.Rejected
	}
	// Everything in a replay comes from the recorded test, nothing live.
	v.Ratio, v.CacheHitRate = "–", "–"
	if run.LimitRatio != nil {
		v.Ratio = formatRatio(*run.LimitRatio)
	}
	if run.CacheHitPct != nil {
		v.CacheHitRate = formatPct(*run.CacheHitPct)
	}
	v.Allowed60, v.Rejected60 = 0, 0
	for _, p := range rp.Points[:shown] {
		v.Allowed60 += p.Allowed
		v.Rejected60 += p.Rejected
	}
	return v
}

// currentReplay loads the showcase run, refreshing at most every 30 seconds
// or right after a run finishes or is pinned.
func (s *Server) currentReplay(ctx context.Context) *Replay {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()
	if !s.replayDirty && time.Since(s.replayAt) < 30*time.Second && !s.replayAt.IsZero() {
		return s.replay
	}
	s.replayAt, s.replayDirty = time.Now(), false
	run, err := s.opt.Store.ShowcaseRun(ctx)
	if err != nil {
		s.replay = nil
		return nil
	}
	secs, err := s.opt.Store.RunSeconds(ctx, run.ID)
	if err != nil {
		return s.replay
	}
	s.replay = NewReplay(run, secs)
	return s.replay
}

func (s *Server) invalidateReplay() {
	s.replayMu.Lock()
	s.replayDirty = true
	s.replayMu.Unlock()
}

// clock formats seconds as m:ss, e.g. 75 -> "1:15".
func clock(sec int) string {
	return itoa(sec/60) + ":" + fmt.Sprintf("%02d", sec%60)
}
