package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/apikey"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/catalog"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/store"
)

var testSigner, _ = apikey.NewSigner("api-test-secret-api-test-secret-api-test")

type fakeKeys struct {
	mu      sync.Mutex
	keys    map[string]store.KeyRecord
	lookups int
	delay   time.Duration
}

func (f *fakeKeys) add(planID int64) string {
	raw, _ := testSigner.Generate()
	f.mu.Lock()
	f.keys[string(apikey.Hash(raw))] = store.KeyRecord{ID: int64(len(f.keys) + 1), PlanID: planID, Prefix: apikey.DisplayPrefix(raw)}
	f.mu.Unlock()
	return raw
}

func (f *fakeKeys) revoke(raw string) {
	f.mu.Lock()
	delete(f.keys, string(apikey.Hash(raw)))
	f.mu.Unlock()
}

func (f *fakeKeys) LookupKey(_ context.Context, h []byte) (store.KeyRecord, bool, error) {
	time.Sleep(f.delay)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	k, ok := f.keys[string(h)]
	return k, ok, nil
}

type staticCatalog struct{ snap *catalog.Snapshot }

func (c *staticCatalog) Current() *catalog.Snapshot { return c.snap }

func newTestServer(t *testing.T) (*Server, *fakeKeys, *staticCatalog) {
	t.Helper()
	keys := &fakeKeys{keys: map[string]store.KeyRecord{}}
	cat := &staticCatalog{snap: &catalog.Snapshot{
		Rules: map[int64]limiter.Rule{
			1: {ID: 1, Name: "free", Algorithm: limiter.TokenBucket, Limit: 10, Window: time.Second, Burst: 10},
		},
		Plans:    map[int64]catalog.Plan{1: {ID: 1, Name: "free", RuleID: 1}},
		Settings: catalog.Settings{FailureMode: limiter.FailFallback},
	}}
	s := New(Options{
		Signer:       testSigner,
		Instance:     "api-test",
		DefaultStore: "memory",
		Keys:         keys,
		Catalog:      cat,
		Memory:       limiter.NewMemory(),
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return s, keys, cat
}

func get(h http.Handler, path, auth string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAuthErrors(t *testing.T) {
	s, keys, _ := newTestServer(t)
	h := s.PublicHandler()
	good := keys.add(1)

	if rec := get(h, "/api/data", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no key: %d", rec.Code)
	}
	if rec := get(h, "/api/data?api_key="+good, ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("key in URL: %d", rec.Code)
	}
	if rec := get(h, "/api/data", "ll_definitely-not-valid"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("malformed: %d", rec.Code)
	}
	if keys.lookups != 0 {
		t.Fatalf("malformed keys must not reach the key store (lookups=%d)", keys.lookups)
	}

	unknown, _ := testSigner.Generate() // validly signed, but not in the store
	for i := 0; i < 5; i++ {
		if rec := get(h, "/api/data", unknown); rec.Code != http.StatusUnauthorized {
			t.Fatalf("unknown key: %d", rec.Code)
		}
	}
	if keys.lookups != 1 {
		t.Fatalf("unknown key should be looked up once then negative-cached, got %d lookups", keys.lookups)
	}
}

func TestRateLimitAndHeaders(t *testing.T) {
	s, keys, _ := newTestServer(t)
	h := s.PublicHandler()
	k := keys.add(1)

	for i := 0; i < 10; i++ {
		rec := get(h, "/api/data", k)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: %d", i, rec.Code)
		}
		if i == 0 {
			if rec.Header().Get("RateLimit-Limit") != "10" || rec.Header().Get("RateLimit-Remaining") != "9" ||
				rec.Header().Get("RateLimit-Reset") != "1" || rec.Header().Get("X-Instance") != "api-test" {
				t.Fatalf("headers: %v", rec.Header())
			}
		}
	}
	rec := get(h, "/api/data", k)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("11th request: %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "1" || rec.Header().Get("RateLimit-Remaining") != "0" {
		t.Fatalf("429 headers: %v", rec.Header())
	}
	if keys.lookups != 1 {
		t.Fatalf("key should be cached after the first lookup, got %d lookups", keys.lookups)
	}
}

func TestRevocationAppliesAfterPurge(t *testing.T) {
	s, keys, _ := newTestServer(t)
	h := s.PublicHandler()
	k := keys.add(1)
	if rec := get(h, "/api/data", k); rec.Code != http.StatusOK {
		t.Fatal(rec.Code)
	}
	keys.revoke(k)
	if rec := get(h, "/api/data", k); rec.Code != http.StatusOK {
		t.Fatal("before the config change notice, the cached key still works (expected)")
	}
	s.PurgeKeyCaches() // what happens when config:version changes
	if rec := get(h, "/api/data", k); rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked key after purge: %d", rec.Code)
	}
}

func TestStatsEndpoint(t *testing.T) {
	s, keys, _ := newTestServer(t)
	k := keys.add(1)
	for i := 0; i < 12; i++ {
		get(s.PublicHandler(), "/api/data", k)
	}
	rec := get(s.InternalHandler(), "/internal/stats", "")
	var st StatsResponse
	if err := json.NewDecoder(rec.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	var allowed, rejected int
	for _, sec := range st.Seconds {
		allowed += sec.Allowed
		rejected += sec.Rejected
	}
	if allowed != 10 || rejected != 2 || len(st.Recent) != 12 || st.Store != "memory" {
		t.Fatalf("stats: allowed=%d rejected=%d recent=%d store=%s", allowed, rejected, len(st.Recent), st.Store)
	}
	if st.Recent[0].Key != apikey.DisplayPrefix(k) {
		t.Fatal("recent requests must show only the key prefix")
	}
	if st.Cache.Hits != 11 || st.Cache.Misses != 1 {
		t.Fatalf("cache stats %+v", st.Cache)
	}
	m := get(s.InternalHandler(), "/metrics", "")
	if m.Code != 200 || !contains(m.Body.String(), "ratelimiter_limiter_duration_seconds_bucket") {
		t.Fatal("metrics endpoint missing limiter histogram")
	}
}

func TestPublicHandlerDoesNotExposeInternals(t *testing.T) {
	s, _, _ := newTestServer(t)
	for _, p := range []string{"/internal/stats", "/metrics"} {
		if rec := get(s.PublicHandler(), p, ""); rec.Code != http.StatusNotFound {
			t.Fatalf("%s on the public port: %d", p, rec.Code)
		}
	}
}

func contains(s, sub string) bool { return strings.Contains(s, sub) }

func TestForgedKeyNeverReachesStore(t *testing.T) {
	s, keys, _ := newTestServer(t)
	other, _ := apikey.NewSigner("someone-elses-secret-someone-elses-secret")
	for i := 0; i < 50; i++ {
		forged, _ := other.Generate()
		if rec := get(s.PublicHandler(), "/api/data", forged); rec.Code != http.StatusUnauthorized {
			t.Fatalf("forged key: %d", rec.Code)
		}
	}
	if keys.lookups != 0 {
		t.Fatalf("forged keys caused %d database lookups", keys.lookups)
	}
}

// Many concurrent requests for an uncached key cause one database lookup.
func TestConcurrentMissesShareOneLookup(t *testing.T) {
	s, keys, _ := newTestServer(t)
	keys.delay = 50 * time.Millisecond
	k := keys.add(1)
	h := s.PublicHandler()
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			get(h, "/api/data", k)
		}()
	}
	wg.Wait()
	if keys.lookups != 1 {
		t.Fatalf("%d lookups for one key, want 1", keys.lookups)
	}
}

type deadRedis struct{}

func (deadRedis) Allow(context.Context, limiter.Rule, string) (limiter.Decision, error) {
	return limiter.Decision{}, errors.New("redis down")
}

func TestFailClosedReturns503(t *testing.T) {
	s, keys, cat := newTestServer(t)
	cat.snap.Settings = catalog.Settings{Store: "redis", FailureMode: limiter.FailClosed}
	s.opt.Redis = limiter.NewResilient(deadRedis{}, limiter.NewBreaker(5, 0.5, time.Second, time.Minute), 15*time.Millisecond, 3,
		func() limiter.FailureMode { return limiter.FailClosed })
	rec := get(s.PublicHandler(), "/api/data", keys.add(1))
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("fail-closed: %d, want 503 with Retry-After", rec.Code)
	}
}
