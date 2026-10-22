package limiter

import (
	"context"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }
func newClock() *fakeClock               { return &fakeClock{t: time.UnixMilli(1_700_000_000_000)} }
func tb(limit, burst int) Rule {
	return Rule{ID: 1, Algorithm: TokenBucket, Limit: limit, Window: time.Second, Burst: burst}
}
func fw(limit int) Rule {
	return Rule{ID: 2, Algorithm: FixedWindow, Limit: limit, Window: time.Second}
}

func countAllowed(t *testing.T, l Limiter, r Rule, n int) int {
	t.Helper()
	allowed := 0
	for i := 0; i < n; i++ {
		d, err := l.Allow(context.Background(), r, "k1")
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			allowed++
		}
	}
	return allowed
}

func TestTokenBucketBurstThenRefill(t *testing.T) {
	c := newClock()
	m := NewMemoryWithClock(c.now)
	r := tb(10, 10)

	if got := countAllowed(t, m, r, 50); got != 10 {
		t.Fatalf("burst: allowed %d, want 10", got)
	}
	d, _ := m.Allow(context.Background(), r, "k1")
	if d.Allowed || d.RetryAfter != 100*time.Millisecond {
		t.Fatalf("expected reject with 100ms retry, got %+v", d)
	}
	c.add(500 * time.Millisecond) // 5 tokens refill
	if got := countAllowed(t, m, r, 50); got != 5 {
		t.Fatalf("after 500ms: allowed %d, want 5", got)
	}
	c.add(10 * time.Second) // refill caps at capacity
	if got := countAllowed(t, m, r, 50); got != 10 {
		t.Fatalf("after long idle: allowed %d, want 10", got)
	}
}

func TestTokenBucketHeadersValues(t *testing.T) {
	c := newClock()
	m := NewMemoryWithClock(c.now)
	d, _ := m.Allow(context.Background(), tb(10, 10), "k1")
	if !d.Allowed || d.Remaining != 9 || d.Limit != 10 || d.Reset != 100*time.Millisecond {
		t.Fatalf("unexpected first decision %+v", d)
	}
}

func TestTokenBucketClampsWhenBurstLowered(t *testing.T) {
	c := newClock()
	m := NewMemoryWithClock(c.now)
	big := tb(100, 100)
	m.Allow(context.Background(), big, "k1") // bucket holds 99 tokens
	small := tb(10, 10)                      // same rule ID, lowered limit
	if got := countAllowed(t, m, small, 200); got != 10 {
		t.Fatalf("after lowering burst: allowed %d, want 10", got)
	}
}

func TestFixedWindow(t *testing.T) {
	c := newClock() // exactly on a second boundary
	m := NewMemoryWithClock(c.now)
	r := fw(10)
	if got := countAllowed(t, m, r, 30); got != 10 {
		t.Fatalf("allowed %d, want 10", got)
	}
	c.add(400 * time.Millisecond)
	d, _ := m.Allow(context.Background(), r, "k1")
	if d.Allowed || d.RetryAfter != 600*time.Millisecond {
		t.Fatalf("expected reject with 600ms retry, got %+v", d)
	}
	c.add(600 * time.Millisecond) // next window
	if got := countAllowed(t, m, r, 30); got != 10 {
		t.Fatalf("next window allowed %d, want 10", got)
	}
}

// The known weakness of fixed windows: a client can get 2x the limit through
// in a short span by bursting at the end of one window and the start of the next.
// A token bucket with the same limit and burst does not allow this.
func TestBoundaryBurst(t *testing.T) {
	c := newClock()
	m := NewMemoryWithClock(c.now)
	c.add(950 * time.Millisecond) // 50ms before the boundary
	first := countAllowed(t, m, fw(10), 30)
	c.add(100 * time.Millisecond) // 50ms after the boundary
	second := countAllowed(t, m, fw(10), 30)
	if first+second != 20 {
		t.Fatalf("fixed window: %d allowed within 100ms, want 20 (2x the limit)", first+second)
	}

	c2 := newClock()
	m2 := NewMemoryWithClock(c2.now)
	c2.add(950 * time.Millisecond)
	a := countAllowed(t, m2, tb(10, 10), 30)
	c2.add(100 * time.Millisecond)
	b := countAllowed(t, m2, tb(10, 10), 30)
	if a+b != 11 {
		t.Fatalf("token bucket: %d allowed within 100ms, want 11 (burst + 100ms of refill)", a+b)
	}
}

func TestSubjectsAndRulesAreIndependent(t *testing.T) {
	m := NewMemoryWithClock(newClock().now)
	r := tb(2, 2)
	ctx := context.Background()
	for _, s := range []string{"a", "b"} {
		for i := 0; i < 2; i++ {
			if d, _ := m.Allow(ctx, r, s); !d.Allowed {
				t.Fatalf("subject %s request %d rejected", s, i)
			}
		}
	}
	r2 := r
	r2.ID = 99
	if d, _ := m.Allow(ctx, r2, "a"); !d.Allowed {
		t.Fatal("different rule should have its own bucket")
	}
}

func TestSweepRemovesIdleEntries(t *testing.T) {
	c := newClock()
	m := NewMemoryWithClock(c.now)
	for _, s := range []string{"a", "b", "c"} {
		m.Allow(context.Background(), tb(10, 10), s)
		m.Allow(context.Background(), fw(10), s)
	}
	if m.Len() != 6 {
		t.Fatalf("len %d, want 6", m.Len())
	}
	c.add(5 * time.Second)
	if n := m.Sweep(); n != 6 || m.Len() != 0 {
		t.Fatalf("sweep removed %d, %d left", n, m.Len())
	}
}

func TestScaled(t *testing.T) {
	r := Rule{Limit: 10, Burst: 10}.Scaled(3)
	if r.Limit != 3 || r.Burst != 3 {
		t.Fatalf("got %+v", r)
	}
	if r := (Rule{Limit: 1}).Scaled(3); r.Limit != 1 {
		t.Fatalf("scaled limit must stay >= 1, got %d", r.Limit)
	}
	if r := (Rule{Limit: 10}).Scaled(1); r.Limit != 10 {
		t.Fatal("scaling by 1 must not change the rule")
	}
}

func TestRuleValidate(t *testing.T) {
	good := Rule{Algorithm: TokenBucket, Limit: 10, Window: time.Second}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := []Rule{
		{Algorithm: "x", Limit: 10, Window: time.Second},
		{Algorithm: TokenBucket, Limit: 0, Window: time.Second},
		{Algorithm: TokenBucket, Limit: 10, Window: time.Millisecond},
		{Algorithm: TokenBucket, Limit: 10, Window: time.Second, Burst: -1},
	}
	for _, r := range bad {
		if r.Validate() == nil {
			t.Errorf("expected %+v to be invalid", r)
		}
	}
}

func TestMaxAllowed(t *testing.T) {
	tb := Rule{Algorithm: TokenBucket, Limit: 50, Window: time.Second, Burst: 100}
	if got := tb.MaxAllowed(10 * time.Second); got != 600 { // 100 burst + 50/s × 10s
		t.Fatalf("token bucket: %v", got)
	}
	fw := Rule{Algorithm: FixedWindow, Limit: 3, Window: 10 * time.Second}
	if got := fw.MaxAllowed(20 * time.Second); got != 9 { // up to 3 windows touched
		t.Fatalf("fixed window: %v", got)
	}
}
