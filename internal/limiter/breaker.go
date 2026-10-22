package limiter

import (
	"sync"
	"time"
)

type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"    // Redis healthy, calls go through
	BreakerOpen     BreakerState = "open"      // Redis failing, calls skipped
	BreakerHalfOpen BreakerState = "half-open" // one trial call allowed
)

// Breaker is a small circuit breaker based on the failure *ratio* in a short
// window, not a count of consecutive failures.
//
// The 15ms timeout alone bounds how long one request waits for Redis. The
// breaker adds something different: once Redis is clearly failing, we stop
// calling it for a cooldown period, so every request doesn't pay the full
// timeout. After the cooldown, one trial request is let through; if it
// succeeds the breaker closes again.
//
// Why a ratio: at 1,000 req/s, a single 15ms pause (a GC pause, a busy CPU)
// makes ~15 requests time out at once. "5 failures in a row" would open the
// breaker for that blip; "at least 10 failures AND at least half of the calls
// in this second failed" doesn't.
type Breaker struct {
	mu          sync.Mutex
	state       BreakerState
	minFailures int
	ratio       float64
	window      time.Duration
	cooldown    time.Duration

	windowStart time.Time
	calls       int
	failures    int

	openedAt time.Time
	probing  bool
	now      func() time.Time
	onChange func(BreakerState)
}

// NewBreaker opens when, within one window, at least minFailures calls
// failed and they were at least ratio of all calls.
func NewBreaker(minFailures int, ratio float64, window, cooldown time.Duration) *Breaker {
	return &Breaker{state: BreakerClosed, minFailures: minFailures, ratio: ratio,
		window: window, cooldown: cooldown, now: time.Now}
}

// OnChange registers a callback for state changes (used for metrics/logging).
func (b *Breaker) OnChange(f func(BreakerState)) { b.onChange = f }

// Allow reports whether a call to Redis should be attempted now.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case BreakerClosed:
		return true
	case BreakerOpen:
		if b.now().Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.setState(BreakerHalfOpen)
		b.probing = true
		return true
	default: // half-open: only one probe at a time
		if b.probing {
			return false
		}
		b.probing = true
		return true
	}
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	if b.state != BreakerClosed {
		b.resetWindow()
		b.setState(BreakerClosed)
		return
	}
	b.count(false)
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
	if b.state == BreakerHalfOpen {
		b.openedAt = b.now()
		b.setState(BreakerOpen)
		return
	}
	if b.state != BreakerClosed {
		return
	}
	b.count(true)
	if b.failures >= b.minFailures && float64(b.failures) >= b.ratio*float64(b.calls) {
		b.openedAt = b.now()
		b.setState(BreakerOpen)
	}
}

func (b *Breaker) count(failed bool) {
	if now := b.now(); now.Sub(b.windowStart) >= b.window {
		b.windowStart, b.calls, b.failures = now, 0, 0
	}
	b.calls++
	if failed {
		b.failures++
	}
}

func (b *Breaker) resetWindow() {
	b.windowStart, b.calls, b.failures = b.now(), 0, 0
}

// Abandon is called when a call that Allow permitted ended without telling
// us anything about Redis (the client hung up before Redis answered). It
// counts as neither a success nor a failure, but if that call was the
// half-open probe, the probe slot is freed so the next request can probe.
// Without this, a cancelled probe would leave the breaker half-open forever
// and Redis would never be used again.
func (b *Breaker) Abandon() {
	b.mu.Lock()
	b.probing = false
	b.mu.Unlock()
}

func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) setState(s BreakerState) {
	b.state = s
	if b.onChange != nil {
		go b.onChange(s)
	}
}
