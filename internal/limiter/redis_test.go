package limiter

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// These unit tests use miniredis, an in-process fake that runs the real Lua
// scripts. The multi-instance correctness test in test/integration runs
// against a real Redis server.
func newMini(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.SetTime(time.UnixMilli(1_700_000_000_000))
	c := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { c.Close() })
	return mr, c
}

func TestRedisTokenBucket(t *testing.T) {
	mr, c := newMini(t)
	l := NewRedis(c)
	if err := l.Preload(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := tb(10, 10)
	if got := countAllowed(t, l, r, 30); got != 10 {
		t.Fatalf("allowed %d, want 10", got)
	}
	d, err := l.Allow(context.Background(), r, "k1")
	if err != nil || d.Allowed || d.RetryAfter != 100*time.Millisecond {
		t.Fatalf("expected reject with 100ms retry, got %+v %v", d, err)
	}
	mr.SetTime(time.UnixMilli(1_700_000_000_000 + 300))
	if got := countAllowed(t, l, r, 30); got != 3 {
		t.Fatalf("after 300ms allowed %d, want 3", got)
	}
	ttl := mr.TTL("rl:tb:1:k1")
	if ttl <= 0 || ttl > 3*time.Second {
		t.Fatalf("bucket key should expire, ttl=%v", ttl)
	}
}

func TestRedisTokenBucketClamp(t *testing.T) {
	_, c := newMini(t)
	l := NewRedis(c)
	l.Allow(context.Background(), tb(100, 100), "k1")
	if got := countAllowed(t, l, tb(10, 10), 100); got != 10 {
		t.Fatalf("after lowering burst: allowed %d, want 10", got)
	}
}

func TestRedisFixedWindow(t *testing.T) {
	mr, c := newMini(t)
	l := NewRedis(c)
	r := fw(10)
	if got := countAllowed(t, l, r, 30); got != 10 {
		t.Fatalf("allowed %d, want 10", got)
	}
	mr.SetTime(time.UnixMilli(1_700_000_000_000 + 999))
	if got := countAllowed(t, l, r, 30); got != 0 {
		t.Fatalf("same window allowed %d more, want 0", got)
	}
	mr.SetTime(time.UnixMilli(1_700_000_000_000 + 1000))
	if got := countAllowed(t, l, r, 30); got != 10 {
		t.Fatalf("next window allowed %d, want 10", got)
	}
	if mr.TTL("rl:fw:2:1000:k1") <= 0 {
		t.Fatal("window key should have an expiry")
	}
}

// Memory and Redis implement the same math, so they must agree.
func TestRedisMatchesMemory(t *testing.T) {
	mr, c := newMini(t)
	rl := NewRedis(c)
	clock := newClock()
	ml := NewMemoryWithClock(clock.now)
	steps := []time.Duration{0, 37 * time.Millisecond, 250 * time.Millisecond, 3 * time.Millisecond, 999 * time.Millisecond, 2 * time.Second}
	for _, rule := range []Rule{tb(7, 4), fw(7)} {
		for i, step := range steps {
			clock.add(step)
			mr.SetTime(clock.t)
			for j := 0; j < 5; j++ {
				a, _ := ml.Allow(context.Background(), rule, "s")
				b, err := rl.Allow(context.Background(), rule, "s")
				if err != nil {
					t.Fatal(err)
				}
				b.Source = a.Source
				if a != b {
					t.Fatalf("%s step %d req %d: memory %+v != redis %+v", rule.Algorithm, i, j, a, b)
				}
			}
		}
	}
}

func TestRedisConcurrentCallsNeverOverAllow(t *testing.T) {
	_, c := newMini(t)
	l := NewRedis(c)
	r := tb(50, 50)
	var mu sync.Mutex
	allowed := 0
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				d, err := l.Allow(context.Background(), r, "shared")
				if err == nil && d.Allowed {
					mu.Lock()
					allowed++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if allowed != 50 {
		t.Fatalf("allowed %d, want exactly 50", allowed)
	}
}

type failingLimiter struct{ calls int }

func (f *failingLimiter) Allow(context.Context, Rule, string) (Decision, error) {
	f.calls++
	return Decision{}, errors.New("redis down")
}

func TestResilientFailureModes(t *testing.T) {
	r := tb(30, 30)
	for _, tc := range []struct {
		mode        FailureMode
		wantAllowed int
		source      string
	}{
		{FailOpen, 100, "fail-open"},
		{FailClosed, 0, "fail-closed"},
		{FailFallback, 10, "fallback"}, // 30 / 3 instances
	} {
		mode := tc.mode
		res := NewResilient(&failingLimiter{}, NewBreaker(5, 0.5, time.Second, time.Minute), 15*time.Millisecond, 3, func() FailureMode { return mode })
		allowed := 0
		for i := 0; i < 100; i++ {
			d, err := res.Allow(context.Background(), r, "k")
			if err != nil {
				t.Fatal(err)
			}
			if d.Source != tc.source {
				t.Fatalf("%s: source %q", mode, d.Source)
			}
			if d.Allowed {
				allowed++
			}
		}
		if allowed != tc.wantAllowed {
			t.Errorf("%s: allowed %d, want %d", mode, allowed, tc.wantAllowed)
		}
	}
}

func TestResilientBreakerStopsCallingRedis(t *testing.T) {
	f := &failingLimiter{}
	res := NewResilient(f, NewBreaker(3, 0.5, time.Second, time.Minute), 15*time.Millisecond, 3, func() FailureMode { return FailOpen })
	for i := 0; i < 100; i++ {
		res.Allow(context.Background(), tb(10, 10), "k")
	}
	if f.calls != 3 {
		t.Fatalf("redis called %d times, want 3 (then the breaker opens)", f.calls)
	}
}

func TestResilientTimeout(t *testing.T) {
	mr, c := newMini(t)
	res := NewResilient(NewRedis(c), NewBreaker(5, 0.5, time.Second, time.Minute), 15*time.Millisecond, 3, func() FailureMode { return FailClosed })
	mr.SetError("") // healthy
	if d, _ := res.Allow(context.Background(), tb(10, 10), "k"); d.Source != "redis" || !d.Allowed {
		t.Fatalf("healthy redis: %+v", d)
	}
	mr.Close() // Redis goes away
	start := time.Now()
	d, _ := res.Allow(context.Background(), tb(10, 10), "k")
	if d.Source != "fail-closed" {
		t.Fatalf("redis down: %+v", d)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("degraded decision took %v", time.Since(start))
	}
}

// slowLimiter fails while down; when healthy it answers after a short delay,
// respecting cancellation like a real network call.
type slowLimiter struct{ healthy bool }

func (f *slowLimiter) Allow(ctx context.Context, r Rule, s string) (Decision, error) {
	if !f.healthy {
		return Decision{}, errors.New("down")
	}
	select {
	case <-ctx.Done():
		return Decision{}, ctx.Err()
	case <-time.After(5 * time.Millisecond):
	}
	return Decision{Allowed: true, Source: "redis"}, nil
}

// Regression test: Redis goes down, the breaker opens, Redis recovers, and the
// client of the single half-open probe hangs up mid-call. The breaker must
// still recover and send traffic back to Redis.
func TestBreakerRecoversAfterCancelledProbe(t *testing.T) {
	c := newClock()
	b := NewBreaker(3, 0.5, time.Second, 2*time.Second)
	b.now = c.now
	f := &slowLimiter{}
	res := NewResilient(f, b, 50*time.Millisecond, 3, func() FailureMode { return FailFallback })
	rule := Rule{ID: 1, Algorithm: TokenBucket, Limit: 10, Window: time.Second}
	for i := 0; i < 5; i++ {
		res.Allow(context.Background(), rule, "k")
	}
	if b.State() != BreakerOpen {
		t.Fatalf("breaker should be open after the outage, is %s", b.State())
	}

	f.healthy = true
	c.add(3 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(time.Millisecond); cancel() }()
	if _, err := res.Allow(ctx, rule, "k"); err == nil {
		t.Fatal("the probe should have been cancelled")
	}

	d, err := res.Allow(context.Background(), rule, "k")
	if err != nil || d.Source != "redis" || b.State() != BreakerClosed {
		t.Fatalf("after a cancelled probe the next request should probe Redis and close the breaker; got source=%q state=%s err=%v",
			d.Source, b.State(), err)
	}
}
