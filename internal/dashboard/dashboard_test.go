package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/api"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/apikey"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/catalog"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/stats"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/store"
)

// fakeStore records mutations so tests can check what happened.
type fakeStore struct {
	mu       sync.Mutex
	settings map[string]string
	showcase *store.Run
	seconds  []store.RunSecond
	revoked  []int64
	lastKey  string
}

func (f *fakeStore) ListRules(context.Context) ([]limiter.Rule, error) {
	return []limiter.Rule{{ID: 1, Name: "free", Algorithm: limiter.TokenBucket, Limit: 10, Window: time.Second, Burst: 10}}, nil
}
func (f *fakeStore) UpdateRule(context.Context, limiter.Rule) error { return nil }
func (f *fakeStore) ListPlans(context.Context) ([]catalog.Plan, error) {
	return []catalog.Plan{{ID: 1, Name: "free", RuleID: 1}}, nil
}
func (f *fakeStore) PlanByName(_ context.Context, n string) (catalog.Plan, error) {
	if n == "free" {
		return catalog.Plan{ID: 1, Name: "free", RuleID: 1}, nil
	}
	return catalog.Plan{}, store.ErrNotFound
}
func (f *fakeStore) Settings(context.Context) (catalog.Settings, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return catalog.Settings{Store: f.settings["store"], FailureMode: limiter.FailFallback}, nil
}
func (f *fakeStore) SetSetting(_ context.Context, k, v string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settings[k] = v
	return nil
}
func (f *fakeStore) CreateKey(_ context.Context, raw, owner string, _ int64, _ *int64) (store.KeyInfo, error) {
	f.mu.Lock()
	f.lastKey = raw
	f.mu.Unlock()
	return store.KeyInfo{Owner: owner, Plan: "free"}, nil
}
func (f *fakeStore) CreateRunKeys(context.Context, []string, int64, int64) error { return nil }
func (f *fakeStore) ListKeys(context.Context, int) ([]store.KeyInfo, error)      { return nil, nil }
func (f *fakeStore) RevokeKey(_ context.Context, id int64) error {
	f.mu.Lock()
	f.revoked = append(f.revoked, id)
	f.mu.Unlock()
	return nil
}
func (f *fakeStore) RevokeRunKeys(context.Context, int64) error                    { return nil }
func (f *fakeStore) CreateRun(context.Context, store.Run) (int64, error)           { return 1, nil }
func (f *fakeStore) FinishRun(context.Context, store.Run, []store.RunSecond) error { return nil }
func (f *fakeStore) ListRuns(context.Context, int) ([]store.Run, error)            { return nil, nil }
func (f *fakeStore) GetRun(context.Context, int64) (store.Run, error) {
	return store.Run{}, store.ErrNotFound
}
func (f *fakeStore) ShowcaseRun(context.Context) (store.Run, error) {
	if f.showcase == nil {
		return store.Run{}, store.ErrNotFound
	}
	return *f.showcase, nil
}
func (f *fakeStore) RunSeconds(context.Context, int64) ([]store.RunSecond, error) {
	return f.seconds, nil
}
func (f *fakeStore) PinShowcase(context.Context, int64, string, *int, string) error { return nil }
func (f *fakeStore) SetRunMarker(context.Context, int64, int, string) error         { return nil }

type countingBumper struct{ n int }

func (b *countingBumper) BumpConfig(context.Context) error { b.n++; return nil }
func (b *countingBumper) BumpKeys(context.Context) error   { b.n++; return nil }

const (
	testUser = "admin"
	testPass = "correct-horse-battery"
)

func newTestDashboard(t *testing.T, instanceURLs ...string) (*Server, *fakeStore, *countingBumper) {
	t.Helper()
	fs := &fakeStore{settings: map[string]string{}}
	b := &countingBumper{}
	s, err := New(Options{
		Store: fs, Bumper: b, Signer: testSigner(t), DemoKey: "ll_demo", InstanceURLs: instanceURLs, LoadTarget: "http://nginx/api/data",
		AdminUser: testUser, AdminPassword: testPass, CSRFSecret: []byte("0123456789abcdef0123456789abcdef"),
		DefaultStore: "redis", MaxRPS: 2000, MaxDuration: time.Minute, MaxClients: 50,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), DisablePolling: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, fs, b
}

func do(h http.Handler, method, path string, form url.Values, auth bool, pass string) *httptest.ResponseRecorder {
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req := httptest.NewRequest(method, path, body)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	if auth {
		req.SetBasicAuth(testUser, pass)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

var adminPosts = []string{"/admin/loadtest", "/admin/settings", "/admin/rules/1", "/admin/keys", "/admin/keys/1/revoke", "/admin/runs/1/pin"}

func TestEveryAdminRouteRequiresAuth(t *testing.T) {
	s, fs, _ := newTestDashboard(t)
	h := s.Handler()
	if rec := do(h, "GET", "/admin", nil, false, ""); rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("GET /admin without auth: %d", rec.Code)
	}
	for _, p := range adminPosts {
		form := url.Values{"csrf_token": {s.auth.csrfToken()}, "store": {"memory"}}
		if rec := do(h, "POST", p, form, false, ""); rec.Code != http.StatusUnauthorized {
			t.Errorf("POST %s without auth: %d", p, rec.Code)
		}
	}
	if len(fs.settings) != 0 || len(fs.revoked) != 0 {
		t.Fatal("unauthenticated requests changed state")
	}
}

func TestEveryAdminPostRequiresCSRF(t *testing.T) {
	s, fs, _ := newTestDashboard(t)
	h := s.Handler()
	for _, p := range adminPosts {
		if rec := do(h, "POST", p, url.Values{"store": {"memory"}}, true, testPass); rec.Code != http.StatusForbidden {
			t.Errorf("POST %s without CSRF token: %d", p, rec.Code)
		}
	}
	// a valid token but a cross-site Origin is still refused
	form := url.Values{"csrf_token": {s.auth.csrfToken()}, "store": {"memory"}}
	req := httptest.NewRequest("POST", "/admin/settings", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", "https://evil.example")
	req.SetBasicAuth(testUser, testPass)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-origin POST: %d", rec.Code)
	}
	if len(fs.settings) != 0 {
		t.Fatal("CSRF-less requests changed state")
	}
}

func TestValidAdminPostWorks(t *testing.T) {
	s, fs, b := newTestDashboard(t)
	form := url.Values{"csrf_token": {s.auth.csrfToken()}, "store": {"memory"}}
	rec := do(s.Handler(), "POST", "/admin/settings", form, true, testPass)
	if rec.Code != http.StatusSeeOther || fs.settings["store"] != "memory" || b.n != 1 {
		t.Fatalf("code=%d settings=%v bumps=%d", rec.Code, fs.settings, b.n)
	}
	form.Set("store", "mongodb")
	rec = do(s.Handler(), "POST", "/admin/settings", form, true, testPass)
	if !strings.Contains(rec.Header().Get("Location"), "err=") || fs.settings["store"] != "memory" {
		t.Fatal("invalid store value must be rejected")
	}
}

func TestCreatedKeyIsNotInURL(t *testing.T) {
	s, fs, _ := newTestDashboard(t)
	form := url.Values{"csrf_token": {s.auth.csrfToken()}, "owner": {"me"}, "plan": {"free"}}
	rec := do(s.Handler(), "POST", "/admin/keys", form, true, testPass)
	if rec.Code != http.StatusOK || rec.Header().Get("Location") != "" || fs.lastKey == "" || !strings.Contains(rec.Body.String(), fs.lastKey) {
		t.Fatalf("key creation should render the key once, code=%d", rec.Code)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("admin responses must not be cached")
	}
}

func TestFailedLoginLockout(t *testing.T) {
	s, _, _ := newTestDashboard(t)
	h := s.Handler()
	for i := 0; i < 5; i++ {
		if rec := do(h, "GET", "/admin", nil, true, "wrong"); rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: %d", i, rec.Code)
		}
	}
	do(h, "GET", "/admin", nil, true, "wrong") // 6th failure triggers the lock
	rec := do(h, "GET", "/admin", nil, true, testPass)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("expected lockout even with the right password, got %d", rec.Code)
	}
	// time passes
	s.auth.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if rec := do(h, "GET", "/admin", nil, true, testPass); rec.Code != http.StatusOK {
		t.Fatalf("after the lock expires: %d", rec.Code)
	}
}

func TestSecurityHeaders(t *testing.T) {
	s, _, _ := newTestDashboard(t)
	rec := do(s.Handler(), "GET", "/", nil, false, "")
	csp := rec.Header().Get("Content-Security-Policy")
	if rec.Code != 200 || !strings.Contains(csp, "frame-ancestors 'none'") || strings.Contains(csp, "unsafe-inline") {
		t.Fatalf("code=%d csp=%q", rec.Code, csp)
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("missing nosniff")
	}
	if strings.Contains(rec.Body.String(), "style=") {
		t.Fatal("inline styles would break the CSP")
	}
	if rec := do(s.Handler(), "GET", "/static/htmx.min.js", nil, false, ""); rec.Code != 200 {
		t.Fatalf("htmx not served: %d", rec.Code)
	}
}

func TestChartEscapesLabels(t *testing.T) {
	out := string(RenderChart([]Point{{1, 2}, {3, 4}}, ChartOptions{Upto: -1, MarkerAt: 1, MarkerLabel: `<script>alert(1)</script>`}))
	if strings.Contains(out, "<script>") || !strings.Contains(out, "&lt;script&gt;") {
		t.Fatal("marker label was not escaped")
	}
	if !strings.Contains(out, `class="allowed"`) || !strings.Contains(out, `class="rejected"`) {
		t.Fatal("missing series")
	}
}

func TestNiceCeil(t *testing.T) {
	for in, want := range map[int]int{0: 4, 3: 4, 5: 8, 30: 40, 31: 40, 9: 20, 99: 100, 101: 200, 950: 1000} {
		if got := niceCeil(in); got != want {
			t.Errorf("niceCeil(%d)=%d want %d", in, got, want)
		}
	}
}

func TestReplayFrameIsSharedAndLoops(t *testing.T) {
	rp := NewReplay(store.Run{DurationS: 10}, []store.RunSecond{{Second: 0, Allowed: 5}, {Second: 0, Allowed: 5, Instance: "b"}})
	if len(rp.Points) != 10 || rp.Points[0].Allowed != 10 {
		t.Fatalf("aggregation: %+v", rp.Points[:1])
	}
	t0 := time.Unix(1_700_000_000, 0)
	if rp.Frame(t0) != rp.Frame(t0) {
		t.Fatal("frame must be deterministic")
	}
	seen := map[int]bool{}
	for i := 0; i < 15; i++ {
		seen[rp.Frame(t0.Add(time.Duration(i)*time.Second))] = true
	}
	if len(seen) != 15 { // 10 seconds of run + 5 seconds pause
		t.Fatalf("loop length %d", len(seen))
	}
}

func fakeInstance(t *testing.T, name string, allowed, rejected int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := time.Now().Unix()
		resp := api.StatsResponse{Instance: name, Store: "redis", Breaker: "closed", FailureMode: "fallback",
			Cache: api.CacheStats{Hits: 99, Misses: 1}}
		for i := int64(0); i < 10; i++ {
			resp.Seconds = append(resp.Seconds, stats.Second{Unix: now - 10 + i, Allowed: allowed, Rejected: rejected})
		}
		resp.Recent = []stats.Request{{At: time.Now(), Key: "ll_abcdefgh", Plan: "free", Status: 429, Source: "redis"}}
		json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestLivePartialAggregatesInstances(t *testing.T) {
	a, b := fakeInstance(t, "api-1", 3, 7), fakeInstance(t, "api-2", 4, 6)
	down := "http://127.0.0.1:1" // nothing listens here
	s, _, _ := newTestDashboard(t, a.URL, b.URL, down)
	s.poller.pollOnce(context.Background())

	v := s.liveView(context.Background(), time.Now().Add(500*time.Millisecond))
	if v.Mode != "live" || v.AllowedNow != 7 || v.RejectedNow != 13 {
		t.Fatalf("mode=%s allowed=%d rejected=%d", v.Mode, v.AllowedNow, v.RejectedNow)
	}
	if v.CacheHitRate != "99.0%" {
		t.Fatalf("hit rate %s", v.CacheHitRate)
	}
	if len(v.Instances) != 3 || v.Instances[2].OK {
		t.Fatal("the unreachable instance should be listed as down")
	}
	rec := do(s.Handler(), "GET", "/partials/live", nil, false, "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "api-2") || !strings.Contains(rec.Body.String(), "unreachable") {
		t.Fatalf("partial: %d", rec.Code)
	}
}

func TestLiveFallsBackToReplay(t *testing.T) {
	s, fs, _ := newTestDashboard(t)
	marker := 3
	fs.showcase = &store.Run{ID: 13, Status: "done", DurationS: 10, StartedAt: time.Date(2026, 10, 13, 9, 0, 0, 0, time.UTC),
		MarkerSecond: &marker, MarkerLabel: "switched to redis"}
	for i := 0; i < 10; i++ {
		fs.seconds = append(fs.seconds, store.RunSecond{Second: i, Instance: "api-1", Allowed: 10, Rejected: 20})
	}
	v := s.liveView(context.Background(), time.Now())
	if v.Mode != "replay" || !strings.Contains(v.Banner, "Replay of a recorded test (13 Oct), not live") || v.ReplayClock == "" {
		t.Fatalf("mode=%s banner=%q", v.Mode, v.Banner)
	}
	if v.Ratio != "–" || v.CacheHitRate != "–" {
		t.Fatalf("an older run without saved results should show dashes, got %q %q", v.Ratio, v.CacheHitRate)
	}

	// The boxes come from the recorded run, even while a live instance is
	// reporting its own cache hit rate.
	live := fakeInstance(t, "api-1", 0, 0)
	s2, fs2, _ := newTestDashboard(t, live.URL)
	s2.poller.pollOnce(context.Background())
	ratio, pct := 0.97, 99.8
	fs2.showcase = &store.Run{ID: 14, Status: "done", DurationS: 10, StartedAt: time.Now(), LimitRatio: &ratio, CacheHitPct: &pct}
	fs2.seconds = fs.seconds
	v2 := s2.liveView(context.Background(), time.Now().Add(time.Hour))
	if v2.Mode != "replay" || v2.Ratio != "0.97×" || v2.CacheHitRate != "99.8%" {
		t.Fatalf("mode=%s ratio=%q cache=%q", v2.Mode, v2.Ratio, v2.CacheHitRate)
	}

	fs.showcase = nil
	s.invalidateReplay()
	if v := s.liveView(context.Background(), time.Now()); v.Mode != "idle" {
		t.Fatalf("no runs: mode %s", v.Mode)
	}
}

func TestLoadTestValidation(t *testing.T) {
	s, _, _ := newTestDashboard(t)
	base := url.Values{"csrf_token": {s.auth.csrfToken()}, "plan": {"free"}, "clients": {"5"}, "rps": {"100"}, "duration": {"10"}, "pattern": {"steady"}}
	for field, bad := range map[string]string{"rps": "5000", "duration": "600", "clients": "0", "pattern": "flood", "plan": "gold"} {
		f := url.Values{}
		for k, v := range base {
			f[k] = v
		}
		f.Set(field, bad)
		rec := do(s.Handler(), "POST", "/admin/loadtest", f, true, testPass)
		if !strings.Contains(rec.Header().Get("Location"), "err=") {
			t.Errorf("%s=%s should be rejected", field, bad)
		}
	}
	if s.runner.active() {
		t.Fatal("no run should have started")
	}
}

func TestRunnerRejectsSecondRun(t *testing.T) {
	s, _, _ := newTestDashboard(t)
	s.runner.mu.Lock()
	s.runner.runID = 99
	s.runner.mu.Unlock()
	_, err := s.runner.start(context.Background(), loadRequest{Clients: 1, RPS: 1, Duration: time.Second, Pattern: "steady"},
		catalog.Plan{ID: 1, Name: "free"}, limiter.Rule{}, "redis")
	if !errors.Is(err, errBusy) {
		t.Fatalf("expected busy, got %v", err)
	}
}

func (f *fakeStore) NoteStoreSwitch(context.Context, int64, string) error { return nil }

func testSigner(t *testing.T) *apikey.Signer {
	t.Helper()
	sg, err := apikey.NewSigner("dashboard-test-secret-dashboard-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	return sg
}

// fakeAPI answers like the real API behind nginx: the first `allow` requests
// get 200, the rest 429, rotating through three instance names.
func fakeAPI(t *testing.T, allow int) (*httptest.Server, *int) {
	t.Helper()
	n := 0
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer ll_demo" {
			t.Errorf("demo key not sent: %q", r.Header.Get("Authorization"))
		}
		mu.Lock()
		n++
		i := n
		mu.Unlock()
		w.Header().Set("X-Instance", "api-"+itoa((i-1)%3+1))
		if i <= allow {
			w.Header().Set("RateLimit-Remaining", itoa(allow-i))
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("RateLimit-Remaining", "0")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func TestTryButton(t *testing.T) {
	s, _, _ := newTestDashboard(t)
	api, sent := fakeAPI(t, 5)
	s.opt.LoadTarget = api.URL

	page := do(s.Handler(), "GET", "/", nil, false, "")
	if !strings.Contains(page.Body.String(), `hx-post="/try"`) || strings.Contains(page.Body.String(), "ll_demo") {
		t.Fatal("the page should show the button and never contain the demo key itself")
	}

	var lines []string
	for i := 0; i < 7; i++ {
		rec := do(s.Handler(), "POST", "/try", url.Values{}, false, "")
		if rec.Code != 200 || rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("click %d: status %d", i, rec.Code)
		}
		lines = append(lines, rec.Body.String())
	}
	if *sent != 7 {
		t.Fatalf("each click should send exactly one request, sent %d", *sent)
	}
	for i, want := range []string{"allowed", "allowed", "allowed", "allowed", "allowed", "blocked", "blocked"} {
		if !strings.Contains(lines[i], want) {
			t.Errorf("click %d: want %q in %q", i+1, want, lines[i])
		}
	}
	if !strings.Contains(lines[0], "answered by api-1") || !strings.Contains(lines[0], "4 left") {
		t.Errorf("first line: %q", lines[0])
	}
	if !strings.Contains(lines[5], "try again in 1s") {
		t.Errorf("blocked line: %q", lines[5])
	}
}

func TestTryButtonAPIDown(t *testing.T) {
	s, _, _ := newTestDashboard(t)
	s.opt.LoadTarget = "http://127.0.0.1:1/api/data"
	rec := do(s.Handler(), "POST", "/try", url.Values{}, false, "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "the API didn") {
		t.Fatalf("API down: %d %q", rec.Code, rec.Body.String())
	}
}

func TestTryButtonHiddenWithoutDemoKey(t *testing.T) {
	s, _, _ := newTestDashboard(t)
	s.opt.DemoKey = ""
	if rec := do(s.Handler(), "POST", "/try", url.Values{}, false, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("POST /try without demo key: %d", rec.Code)
	}
	if strings.Contains(do(s.Handler(), "GET", "/", nil, false, "").Body.String(), "Send a request") {
		t.Fatal("button should be hidden")
	}
}

func TestTryButtonEscapesAPIHeaders(t *testing.T) {
	s, _, _ := newTestDashboard(t)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Instance", `<script>alert(1)</script>`)
		w.Header().Set("RateLimit-Remaining", `<b>`)
	}))
	defer api.Close()
	s.opt.LoadTarget = api.URL
	body := do(s.Handler(), "POST", "/try", url.Values{}, false, "").Body.String()
	if strings.Contains(body, "<script>") || strings.Contains(body, "<b>") {
		t.Fatalf("unescaped header values: %q", body)
	}
}

func TestTryButtonBusy(t *testing.T) {
	s, _, _ := newTestDashboard(t)
	for i := 0; i < tryMaxInFlight; i++ {
		s.trySlots <- struct{}{}
	}
	body := do(s.Handler(), "POST", "/try", url.Values{}, false, "").Body.String()
	if !strings.Contains(body, "busy") {
		t.Fatalf("expected busy message, got %q", body)
	}
}

func TestDescribeLimit(t *testing.T) {
	for want, r := range map[string]limiter.Rule{
		"3 requests every 10 seconds": {Limit: 3, Window: 10 * time.Second},
		"5 requests per second":       {Limit: 5, Window: time.Second},
		"1 request every 2 seconds":   {Limit: 1, Window: 2 * time.Second},
		"10 requests every 500ms":     {Limit: 10, Window: 500 * time.Millisecond},
	} {
		if got := describeLimit(r); got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

// What a real browser sends when the admin submits a form on the dashboard:
// the dashboard's own Origin. This must pass (it failed in Chrome while the
// Referrer-Policy was no-referrer, which makes browsers send "Origin: null").
func TestBrowserFormPostFromSameOrigin(t *testing.T) {
	s, fs, _ := newTestDashboard(t)
	page := do(s.Handler(), "GET", "/admin", nil, true, testPass)
	if rp := page.Header().Get("Referrer-Policy"); rp != "same-origin" {
		t.Fatalf("Referrer-Policy %q: browsers would send Origin: null and every form would fail", rp)
	}
	form := url.Values{"csrf_token": {s.auth.csrfToken()}, "store": {"memory"}}
	req := httptest.NewRequest("POST", "http://localhost:8000/admin/settings", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", "http://localhost:8000")
	req.SetBasicAuth(testUser, testPass)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || fs.settings["store"] != "memory" {
		t.Fatalf("same-origin browser POST: %d", rec.Code)
	}
}

func TestClock(t *testing.T) {
	for in, want := range map[int]string{0: "0:00", 9: "0:09", 40: "0:40", 75: "1:15"} {
		if got := clock(in); got != want {
			t.Errorf("clock(%d)=%q want %q", in, got, want)
		}
	}
}

func TestLiveBoxesDuringATest(t *testing.T) {
	a := fakeInstance(t, "api-1", 3, 7)
	s, _, _ := newTestDashboard(t, a.URL)
	s.poller.pollOnce(context.Background())
	now := time.Now()
	s.runner.mu.Lock()
	s.runner.runID, s.runner.started, s.runner.clients = 1, now.Add(-5*time.Second), 1
	s.runner.rule = limiter.Rule{Algorithm: limiter.TokenBucket, Limit: 3, Window: time.Second, Burst: 3}
	s.runner.mu.Unlock()

	v := s.liveView(context.Background(), now)
	if !v.SinceStart || v.Allowed60 == 0 || v.Allowed60 > 3*7 {
		t.Fatalf("since start %v, allowed %d", v.SinceStart, v.Allowed60)
	}
	if !strings.HasSuffix(v.Ratio, "×") {
		t.Fatalf("ratio %q", v.Ratio)
	}
}

func TestComma(t *testing.T) {
	for in, want := range map[int]string{0: "0", 999: "999", 1045: "1,045", 1234567: "1,234,567", -2500: "-2,500"} {
		if got := comma(in); got != want {
			t.Errorf("comma(%d)=%q want %q", in, got, want)
		}
	}
}
