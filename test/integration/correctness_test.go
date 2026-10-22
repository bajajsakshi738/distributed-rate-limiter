//go:build integration

// Package integration runs against real Redis (and Postgres for store tests).
//
//	REDIS_ADDR=localhost:6379 DATABASE_URL=postgres://... go test -tags integration ./test/integration/ -v
package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/api"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/apikey"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/catalog"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/loadgen"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/store"
)

func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set")
	}
	c := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("REDIS_PASSWORD"), ContextTimeoutEnabled: true, MaxRetries: -1})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis not reachable at %s: %v", addr, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

var signer, _ = apikey.NewSigner("integration-secret-integration-secret-123")

type oneKey struct{ hash string }

func (k oneKey) LookupKey(_ context.Context, h []byte) (store.KeyRecord, bool, error) {
	if string(h) == k.hash {
		return store.KeyRecord{ID: 1, PlanID: 1, Prefix: "ll_test"}, true, nil
	}
	return store.KeyRecord{}, false, nil
}

type fixedCatalog struct{ s *catalog.Snapshot }

func (c fixedCatalog) Current() *catalog.Snapshot { return c.s }

// cluster starts n API instances (each with its own memory limiter and its
// own Redis connection pool, like separate processes) behind a round-robin
// reverse proxy, the way nginx is configured in production.
func cluster(t *testing.T, n int, storeName string, rule limiter.Rule, rdb *redis.Client, keyHash string) string {
	t.Helper()
	snap := &catalog.Snapshot{
		Rules:    map[int64]limiter.Rule{rule.ID: rule},
		Plans:    map[int64]catalog.Plan{1: {ID: 1, Name: "free", RuleID: rule.ID}},
		Settings: catalog.Settings{Store: storeName, FailureMode: limiter.FailClosed},
	}
	var backends []*url.URL
	for i := 0; i < n; i++ {
		inst := redis.NewClient(rdb.Options())
		t.Cleanup(func() { inst.Close() })
		srv := api.New(api.Options{
			Signer:       signer,
			Instance:     fmt.Sprintf("api-%d", i+1),
			DefaultStore: storeName,
			Keys:         oneKey{hash: keyHash},
			Catalog:      fixedCatalog{snap},
			Memory:       limiter.NewMemory(),
			Redis: limiter.NewResilient(limiter.NewRedis(inst), limiter.NewBreaker(5, 0.5, time.Second, 2*time.Second),
				100*time.Millisecond, n, func() limiter.FailureMode { return limiter.FailClosed }),
			Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		hs := httptest.NewServer(srv.PublicHandler())
		t.Cleanup(hs.Close)
		u, _ := url.Parse(hs.URL)
		backends = append(backends, u)
	}
	var next atomic.Uint64
	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(backends[next.Add(1)%uint64(len(backends))])
		},
		Transport: &http.Transport{MaxIdleConnsPerHost: 500},
	}
	lb := httptest.NewServer(proxy)
	t.Cleanup(lb.Close)
	return lb.URL + "/api/data"
}

func runLoad(t *testing.T, target, key string, rps int, d time.Duration) loadgen.Result {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 500}}
	res, err := loadgen.Run(context.Background(), loadgen.Config{
		Target: target, Keys: []string{key}, RPS: rps, Duration: d, Pattern: loadgen.PatternSteady,
	}, client)
	if err != nil {
		t.Fatal(err)
	}
	if res.Errors > 0 {
		t.Fatalf("%d transport/server errors", res.Errors)
	}
	return res
}

// The core claim of the project, tested end to end through a load balancer:
// with Redis, 3 instances together never allow more than the token bucket
// permits (burst + rate × time); in memory mode they allow about 3x that.
func TestMultiInstanceCorrectness(t *testing.T) {
	rdb := redisClient(t)
	key, _ := signer.Generate()
	hash := string(apikey.Hash(key))
	const (
		limit = 10
		rps   = 100 // 10x over the limit
	)
	duration := 6 * time.Second

	for _, tc := range []struct {
		store string
		check func(t *testing.T, allowed int64, bound float64)
	}{
		{"redis", func(t *testing.T, allowed int64, bound float64) {
			if float64(allowed) > bound {
				t.Fatalf("redis mode allowed %d, more than the bound %.1f", allowed, bound)
			}
			if float64(allowed) < 0.9*bound {
				t.Fatalf("redis mode allowed %d, suspiciously far below the bound %.1f", allowed, bound)
			}
		}},
		{"memory", func(t *testing.T, allowed int64, bound float64) {
			// each of the 3 instances runs its own bucket: ~3x the bound
			if float64(allowed) < 2.5*bound {
				t.Fatalf("memory mode allowed %d; expected about 3x the bound %.1f (the bug)", allowed, bound)
			}
		}},
	} {
		t.Run(tc.store, func(t *testing.T) {
			rule := limiter.Rule{ID: time.Now().UnixNano(), Algorithm: limiter.TokenBucket, Limit: limit, Window: time.Second, Burst: limit}
			target := cluster(t, 3, tc.store, rule, rdb, hash)
			res := runLoad(t, target, key, rps, duration)
			// Bound for a token bucket over the observed span: burst + rate × time.
			// +1 allows for a token refilling during the last request's flight.
			bound := float64(limit) + float64(limit)*res.Elapsed.Seconds() + 1
			t.Logf("%s: sent %d, allowed %d, rejected %d over %v (bound %.1f, ratio %.2fx), peak 1s %d, sources %v",
				tc.store, res.Sent, res.Allowed, res.Rejected, res.Elapsed.Round(time.Millisecond), bound,
				float64(res.Allowed)/bound, res.Peak1sAllowed, res.Sources)
			tc.check(t, res.Allowed, bound)

			// Every allowed request must have been decided by the store under
			// test. Otherwise a breaker opening mid-test could let fallback
			// mode pass for Redis mode.
			if res.Sources[tc.store] != res.Allowed {
				t.Fatalf("allowed responses by source %v; want all %d from %q", res.Sources, res.Allowed, tc.store)
			}
			// The limit must hold over every 1-second span, not just in total.
			if tc.store == "redis" && res.Peak1sAllowed > limit+limit+1 {
				t.Fatalf("peak in a 1s span %d > burst + rate + 1 = %d", res.Peak1sAllowed, 2*limit+1)
			}
			// Each instance must have received about a third of the traffic;
			// otherwise a proxy sending everything to one instance would make
			// any limiter look correct.
			perInstance := map[string]int{}
			for _, b := range res.Buckets {
				perInstance[b.Instance] += b.Allowed + b.Rejected
			}
			if len(perInstance) != 3 {
				t.Fatalf("traffic reached %d instances, want 3: %v", len(perInstance), perInstance)
			}
			for inst, n := range perInstance {
				if share := float64(n) / float64(res.Sent); share < 0.25 || share > 0.42 {
					t.Fatalf("instance %s got %.0f%% of requests, want about a third", inst, share*100)
				}
			}
		})
	}
}

// Fixed window against real Redis: the boundary burst lets through up to 2x
// the limit in well under a second; a token bucket with the same limit doesn't.
func TestBoundaryBurstRealRedis(t *testing.T) {
	rdb := redisClient(t)
	key, _ := signer.Generate()
	hash := string(apikey.Hash(key))
	for _, tc := range []struct {
		algo    limiter.Algorithm
		minPeak int
		maxPeak int
	}{
		{limiter.FixedWindow, 19, 20},
		{limiter.TokenBucket, 10, 12},
	} {
		t.Run(string(tc.algo), func(t *testing.T) {
			rule := limiter.Rule{ID: time.Now().UnixNano(), Algorithm: tc.algo, Limit: 10, Window: time.Second, Burst: 10}
			target := cluster(t, 3, "redis", rule, rdb, hash)
			client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 500}}
			res, err := loadgen.Run(context.Background(), loadgen.Config{
				Target: target, Keys: []string{key}, RPS: 30, Duration: 5 * time.Second, Pattern: loadgen.PatternBoundary,
			}, client)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s: peak allowed in any 1s span = %d (limit 10)", tc.algo, res.Peak1sAllowed)
			if res.Peak1sAllowed < tc.minPeak || res.Peak1sAllowed > tc.maxPeak {
				t.Fatalf("peak %d outside [%d, %d]", res.Peak1sAllowed, tc.minPeak, tc.maxPeak)
			}
		})
	}
}

// With Redis unreachable, fallback mode limits each instance to its share
// of the limit, and the circuit breaker keeps decisions fast.
func TestRedisOutageFallback(t *testing.T) {
	// A client pointing at a port where nothing listens behaves like a dead Redis.
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", ContextTimeoutEnabled: true, MaxRetries: -1, DialTimeout: 20 * time.Millisecond})
	defer dead.Close()
	rule := limiter.Rule{ID: 7, Algorithm: limiter.TokenBucket, Limit: 30, Window: time.Second, Burst: 30}
	res := limiter.NewResilient(limiter.NewRedis(dead), limiter.NewBreaker(3, 0.5, time.Second, time.Second), 15*time.Millisecond, 3,
		func() limiter.FailureMode { return limiter.FailFallback })
	allowed := 0
	start := time.Now()
	for i := 0; i < 100; i++ {
		d, _ := res.Allow(context.Background(), rule, "k")
		if d.Allowed {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("fallback allowed %d, want 10 (30 / 3 instances)", allowed)
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("100 degraded decisions took %v; the breaker should make them fast", el)
	}
}
