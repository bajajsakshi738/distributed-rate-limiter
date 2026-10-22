package limiter

import (
	"testing"
	"time"
)

func newTestBreaker(c *fakeClock) *Breaker {
	b := NewBreaker(3, 0.5, time.Second, 2*time.Second)
	b.now = c.now
	return b
}

func TestBreakerLifecycle(t *testing.T) {
	c := newClock()
	b := newTestBreaker(c)

	for i := 0; i < 2; i++ {
		if !b.Allow() {
			t.Fatal("closed breaker must allow")
		}
		b.Failure()
	}
	if b.State() != BreakerClosed {
		t.Fatal("2 failures should not open a min-3 breaker")
	}
	b.Allow()
	b.Failure()
	if b.State() != BreakerOpen || b.Allow() {
		t.Fatal("3 failures out of 3 calls should open the breaker and block calls")
	}

	c.add(2 * time.Second)
	if !b.Allow() || b.State() != BreakerHalfOpen {
		t.Fatal("after cooldown one probe should be allowed")
	}
	if b.Allow() {
		t.Fatal("only one probe at a time in half-open")
	}
	b.Failure()
	if b.State() != BreakerOpen {
		t.Fatal("failed probe should reopen")
	}

	c.add(2 * time.Second)
	b.Allow()
	b.Success()
	if b.State() != BreakerClosed || !b.Allow() {
		t.Fatal("successful probe should close the breaker")
	}
}

// A short stall at high traffic produces a burst of failures that are a
// small fraction of all calls. That must not open the breaker.
func TestBreakerIgnoresBlipsAtHighTraffic(t *testing.T) {
	c := newClock()
	b := newTestBreaker(c)
	for i := 0; i < 1000; i++ {
		b.Success()
	}
	for i := 0; i < 15; i++ { // a 15ms stall at 1,000 req/s
		b.Failure()
	}
	if b.State() != BreakerClosed {
		t.Fatal("15 failures among 1,015 calls should not open the breaker")
	}
}

func TestBreakerWindowResets(t *testing.T) {
	c := newClock()
	b := newTestBreaker(c)
	b.Failure()
	b.Failure()
	c.add(1100 * time.Millisecond) // new window
	b.Failure()
	if b.State() != BreakerClosed {
		t.Fatal("failures in different windows must not add up")
	}
}
