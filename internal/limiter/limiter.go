// Package limiter contains the rate-limiting algorithms (token bucket and
// fixed window) with two storage backends: per-process memory and shared
// Redis, plus a resilient wrapper that decides what to do when Redis fails.
package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

type Algorithm string

const (
	TokenBucket Algorithm = "token_bucket"
	FixedWindow Algorithm = "fixed_window"
)

// Rule describes a limit, e.g. "10 requests per 1s, burst 10".
//
// Token bucket: refills Limit tokens per Window, holds at most Burst tokens.
// Fixed window: allows Limit requests per aligned Window.
type Rule struct {
	ID        int64
	Name      string
	Algorithm Algorithm
	Limit     int
	Window    time.Duration
	Burst     int
}

func (r Rule) Validate() error {
	switch r.Algorithm {
	case TokenBucket, FixedWindow:
	default:
		return fmt.Errorf("unknown algorithm %q", r.Algorithm)
	}
	if r.Limit < 1 || r.Limit > 1_000_000 {
		return errors.New("limit must be between 1 and 1,000,000")
	}
	if r.Window < 100*time.Millisecond || r.Window > 24*time.Hour {
		return errors.New("window must be between 100ms and 24h")
	}
	if r.Burst < 0 || r.Burst > 1_000_000 {
		return errors.New("burst must be between 0 and 1,000,000")
	}
	return nil
}

// Capacity is the maximum burst for a token bucket. Burst 0 means "same as limit".
func (r Rule) Capacity() int {
	if r.Burst > 0 {
		return r.Burst
	}
	return r.Limit
}

// MaxAllowed is the most requests one key can get through in the given time
// if the limit is enforced exactly: the full burst plus the refill (token
// bucket), or one full window per window touched (fixed window). Load tests
// compare what actually got through against this.
func (r Rule) MaxAllowed(elapsed time.Duration) float64 {
	windows := float64(elapsed) / float64(r.Window)
	if r.Algorithm == FixedWindow {
		return float64(r.Limit) * (windows + 1)
	}
	return float64(r.Capacity()) + float64(r.Limit)*windows
}

// RatePerMs is the token refill rate.
func (r Rule) RatePerMs() float64 {
	return float64(r.Limit) / float64(r.Window.Milliseconds())
}

// Scaled divides the rule between n instances. Used by the fallback limiter
// when Redis is down: each instance enforces roughly 1/n of the limit on its
// own, so the total stays approximately right without shared state.
// Both the rate and the burst are divided. Known approximations: each share
// is at least 1, so a limit smaller than n over-allows (limit 1 across 3
// instances allows 3); rounding can under-allow (10/3 -> 3 each, 9 total);
// and if an instance dies, the survivors still use 1/n each, so the total
// drops to (n-1)/n of the limit.
func (r Rule) Scaled(n int) Rule {
	if n <= 1 {
		return r
	}
	s := r
	s.Limit = max(1, int(math.Round(float64(r.Limit)/float64(n))))
	if r.Burst > 0 {
		s.Burst = max(1, int(math.Round(float64(r.Burst)/float64(n))))
	}
	return s
}

// Decision is the limiter's answer for one request.
type Decision struct {
	Allowed    bool
	Limit      int
	Remaining  int
	Reset      time.Duration // token bucket: until full; fixed window: until the window ends
	RetryAfter time.Duration // only meaningful when !Allowed
	Source     string        // memory | redis | fallback | fail-open | fail-closed
}

// Limiter decides whether subject (an API key ID) may make one more request.
type Limiter interface {
	Allow(ctx context.Context, rule Rule, subject string) (Decision, error)
}
