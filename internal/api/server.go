// Package api is the protected API service: one endpoint, /api/data, behind
// the rate-limit middleware.
package api

import (
	"context"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/singleflight"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/apikey"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/catalog"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/httpx"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/keycache"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/stats"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/store"
)

// KeyStore looks up API keys by hash (Postgres in production).
type KeyStore interface {
	LookupKey(ctx context.Context, hash []byte) (store.KeyRecord, bool, error)
}

// Catalog returns the current rules, plans and settings.
type Catalog interface {
	Current() *catalog.Snapshot
}

type lookupResult struct {
	rec   store.KeyRecord
	found bool
}

type Options struct {
	Signer       *apikey.Signer
	Instance     string
	DefaultStore string // LIMITER_STORE: used until the dashboard sets one
	Keys         KeyStore
	Catalog      Catalog
	Memory       *limiter.Memory
	Redis        *limiter.Resilient
	Log          *slog.Logger
	CacheSize    int
	CacheTTL     time.Duration
}

type Server struct {
	opt     Options
	pos     *keycache.LRU[string, store.KeyRecord]
	lookups singleflight.Group
	neg     *keycache.LRU[string, struct{}]
	stats   *stats.Recorder
	metrics *Metrics
	started time.Time

	cacheHits, cacheMisses, negHits, malformed atomic.Int64
}

func New(opt Options) *Server {
	if opt.CacheSize == 0 {
		opt.CacheSize = 100_000
	}
	if opt.CacheTTL == 0 {
		// Also the worst-case time for a revocation to apply if Redis is down
		// and the change notification is missed.
		opt.CacheTTL = 60 * time.Second
	}
	s := &Server{
		opt:     opt,
		pos:     keycache.New[string, store.KeyRecord](opt.CacheSize, opt.CacheTTL),
		neg:     keycache.New[string, struct{}](10_000, opt.CacheTTL),
		stats:   stats.NewRecorder(),
		metrics: NewMetrics(),
		started: time.Now(),
	}
	if opt.Redis != nil {
		opt.Redis.Breaker().OnChange(func(st limiter.BreakerState) {
			v := 0.0
			if st != limiter.BreakerClosed {
				v = 1
			}
			s.metrics.BreakerOpen.Set(v)
			opt.Log.Warn("redis circuit breaker state changed", "state", st)
		})
	}
	return s
}

// PurgeKeyCaches is called when config:version changes, so a revoked key
// stops working within ~2 seconds and a "not found" result doesn't stick to
// a key that was just created.
func (s *Server) PurgeKeyCaches() {
	s.pos.Purge()
	s.neg.Purge()
}

func (s *Server) Metrics() *Metrics { return s.metrics }

// PublicHandler is what nginx exposes to the internet.
func (s *Server) PublicHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /api/data", s.rateLimit(http.HandlerFunc(s.handleData)))
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return httpx.LimitBody(1<<10, mux)
}

// InternalHandler is only reachable on the private Docker network.
func (s *Server) InternalHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/stats", s.handleStats)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if s.opt.Catalog.Current() == nil {
		httpx.Error(w, http.StatusServiceUnavailable, "config not loaded")
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]string{"status": "ok", "instance": s.opt.Instance})
}

func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	httpx.JSON(w, http.StatusOK, map[string]any{
		"city":      "Example City",
		"temp_c":    27.4,
		"condition": "partly cloudy",
		"served_by": s.opt.Instance,
	})
}

// activeStore returns "memory" or "redis".
func (s *Server) activeStore(snap *catalog.Snapshot) string {
	if snap.Settings.Store != "" {
		return snap.Settings.Store
	}
	return s.opt.DefaultStore
}

func (s *Server) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		w.Header().Set("X-Instance", s.opt.Instance)

		// Keys in URLs end up in access logs and browser history.
		q := r.URL.Query()
		if q.Has("api_key") || q.Has("key") || q.Has("token") {
			httpx.Error(w, http.StatusBadRequest, "send the API key in the Authorization header, not the URL")
			return
		}
		raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || raw == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="ratelimiter"`)
			httpx.Error(w, http.StatusUnauthorized, "missing API key")
			return
		}
		if s.opt.Signer.Validate(raw) != nil {
			// Made-up keys, typos and junk are rejected here, before any cache
			// or database work: only keys signed with KEY_SECRET get past.
			s.malformed.Add(1)
			s.metrics.KeyCache.WithLabelValues("malformed").Inc()
			httpx.Error(w, http.StatusUnauthorized, "invalid API key")
			return
		}

		key, found, err := s.lookup(raw)
		if err != nil {
			s.opt.Log.Error("key lookup failed", "err", err)
			httpx.Error(w, http.StatusServiceUnavailable, "temporarily unavailable")
			return
		}
		if !found {
			httpx.Error(w, http.StatusUnauthorized, "invalid API key")
			return
		}

		snap := s.opt.Catalog.Current()
		rule, plan, ok := snap.RuleForPlan(key.PlanID)
		if !ok {
			s.opt.Log.Error("plan has no rule", "plan_id", key.PlanID)
			httpx.Error(w, http.StatusInternalServerError, "internal error")
			return
		}

		storeName := s.activeStore(snap)
		var lim limiter.Limiter = s.opt.Memory
		if storeName == "redis" && s.opt.Redis != nil {
			lim = s.opt.Redis
		}
		subject := strconv.FormatInt(key.ID, 10)
		limStart := time.Now()
		d, err := lim.Allow(r.Context(), rule, subject)
		if err != nil {
			return // client went away
		}
		// Labelled by what actually decided, so the "redis" series is pure
		// Redis round trips; time lost to a failing Redis shows up under
		// "fallback" / "fail-open" / "fail-closed" instead.
		s.metrics.LimiterDuration.WithLabelValues(d.Source).Observe(time.Since(limStart).Seconds())

		setRateLimitHeaders(w, d)
		status := http.StatusOK
		if d.Allowed {
			next.ServeHTTP(w, r)
		} else if d.Source == "fail-closed" {
			// The client didn't exceed anything: the limiter can't decide
			// because Redis is down. 429 would wrongly blame the client.
			status = http.StatusServiceUnavailable
			w.Header().Set("Retry-After", strconv.Itoa(ceilSeconds(d.RetryAfter)))
			httpx.Error(w, status, "rate limiter unavailable")
		} else {
			status = http.StatusTooManyRequests
			w.Header().Set("Retry-After", strconv.Itoa(ceilSeconds(d.RetryAfter)))
			httpx.Error(w, status, "rate limit exceeded")
		}

		elapsed := time.Since(start)
		decision := "allowed"
		if !d.Allowed {
			decision = "rejected"
		}
		s.metrics.Requests.WithLabelValues(decision, d.Source).Inc()
		s.metrics.RequestDuration.Observe(elapsed.Seconds())
		s.stats.Record(stats.Request{
			At:        start,
			Key:       key.Prefix,
			Plan:      plan.Name,
			Status:    status,
			Remaining: d.Remaining,
			Source:    d.Source,
			LatencyUs: elapsed.Microseconds(),
		}, d.Allowed, d.Source != "redis" && d.Source != "memory")
	})
}

func (s *Server) lookup(raw string) (store.KeyRecord, bool, error) {
	h := string(apikey.Hash(raw))
	if k, ok := s.pos.Get(h); ok {
		s.cacheHits.Add(1)
		s.metrics.KeyCache.WithLabelValues("hit").Inc()
		return k, true, nil
	}
	if _, ok := s.neg.Get(h); ok {
		s.negHits.Add(1)
		s.metrics.KeyCache.WithLabelValues("negative_hit").Inc()
		return store.KeyRecord{}, false, nil
	}
	s.cacheMisses.Add(1)
	s.metrics.KeyCache.WithLabelValues("miss").Inc()

	// singleflight: if many requests for the same key miss the cache at the
	// same moment (e.g. right after the caches are cleared), only one of
	// them queries Postgres and the rest share its answer. The query isn't
	// tied to any one request's context, so a client hanging up early
	// doesn't fail the others.
	v, err, _ := s.lookups.Do(h, func() (any, error) {
		lctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		k, found, err := s.opt.Keys.LookupKey(lctx, []byte(h))
		return lookupResult{k, found}, err
	})
	if err != nil {
		return store.KeyRecord{}, false, err
	}
	res := v.(lookupResult)
	k, found := res.rec, res.found
	if found {
		s.pos.Add(h, k)
	} else {
		s.neg.Add(h, struct{}{})
	}
	return k, found, nil
}

// setRateLimitHeaders uses the three-field style (RateLimit-Limit,
// RateLimit-Remaining, RateLimit-Reset) from the earlier IETF
// "RateLimit header fields for HTTP" drafts. Reset is in seconds.
func setRateLimitHeaders(w http.ResponseWriter, d limiter.Decision) {
	h := w.Header()
	h.Set("RateLimit-Limit", strconv.Itoa(d.Limit))
	h.Set("RateLimit-Remaining", strconv.Itoa(max(0, d.Remaining)))
	h.Set("RateLimit-Reset", strconv.Itoa(ceilSeconds(d.Reset)))
	h.Set("X-RateLimit-Source", d.Source)
}

// ceilSeconds rounds up to whole seconds, with a minimum of 1 for waits, since
// Retry-After: 0 would invite an immediate retry that will also fail.
func ceilSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return max(1, int(math.Ceil(d.Seconds())))
}

// ---- /internal/stats ----

type CacheStats struct {
	Hits         int64 `json:"hits"`
	Misses       int64 `json:"misses"`
	NegativeHits int64 `json:"negative_hits"`
	Malformed    int64 `json:"malformed"`
}

type StatsResponse struct {
	Instance    string          `json:"instance"`
	Store       string          `json:"store"`
	FailureMode string          `json:"failure_mode"`
	Breaker     string          `json:"breaker"`
	UptimeS     int64           `json:"uptime_s"`
	Seconds     []stats.Second  `json:"seconds"`
	Recent      []stats.Request `json:"recent"`
	Cache       CacheStats      `json:"cache"`
}

func (s *Server) Stats(now time.Time) StatsResponse {
	snap := s.opt.Catalog.Current()
	resp := StatsResponse{
		Instance: s.opt.Instance,
		UptimeS:  int64(now.Sub(s.started).Seconds()),
		Seconds:  s.stats.Seconds(now, stats.WindowSeconds),
		Recent:   s.stats.Recent(),
		Cache: CacheStats{
			Hits:         s.cacheHits.Load(),
			Misses:       s.cacheMisses.Load(),
			NegativeHits: s.negHits.Load(),
			Malformed:    s.malformed.Load(),
		},
		Breaker: "n/a",
	}
	if snap != nil {
		resp.Store = s.activeStore(snap)
		resp.FailureMode = string(snap.Settings.FailureMode)
	}
	if s.opt.Redis != nil {
		resp.Breaker = string(s.opt.Redis.Breaker().State())
	}
	return resp
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	httpx.JSON(w, http.StatusOK, s.Stats(time.Now()))
}
