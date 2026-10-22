package limiter

import (
	"context"
	"time"
)

type FailureMode string

const (
	FailFallback FailureMode = "fallback" // limit locally at limit / instance count
	FailOpen     FailureMode = "open"     // allow everything
	FailClosed   FailureMode = "closed"   // reject everything
)

func (m FailureMode) Valid() bool {
	return m == FailFallback || m == FailOpen || m == FailClosed
}

// Resilient calls Redis with a tight timeout behind a circuit breaker, and
// applies the configured failure mode when Redis can't answer.
type Resilient struct {
	primary   Limiter
	fallback  *Memory
	breaker   *Breaker
	timeout   time.Duration
	instances int
	mode      func() FailureMode
}

func NewResilient(primary Limiter, breaker *Breaker, timeout time.Duration, instances int, mode func() FailureMode) *Resilient {
	return &Resilient{
		primary:   primary,
		fallback:  NewMemory(),
		breaker:   breaker,
		timeout:   timeout,
		instances: instances,
		mode:      mode,
	}
}

func (r *Resilient) Breaker() *Breaker { return r.breaker }
func (r *Resilient) Fallback() *Memory { return r.fallback }

func (r *Resilient) Allow(ctx context.Context, rule Rule, subject string) (Decision, error) {
	if r.breaker.Allow() {
		cctx, cancel := context.WithTimeout(ctx, r.timeout)
		d, err := r.primary.Allow(cctx, rule, subject)
		cancel()
		if err == nil {
			r.breaker.Success()
			return d, nil
		}
		// If the client went away, that says nothing about Redis's health,
		// but the breaker must be told the call is over (see Abandon).
		if ctx.Err() != nil {
			r.breaker.Abandon()
			return Decision{}, ctx.Err()
		}
		r.breaker.Failure()
	}
	return r.degraded(ctx, rule, subject), nil
}

func (r *Resilient) degraded(ctx context.Context, rule Rule, subject string) Decision {
	switch r.mode() {
	case FailOpen:
		return Decision{Allowed: true, Limit: rule.Limit, Remaining: rule.Limit, Source: "fail-open"}
	case FailClosed:
		return Decision{Allowed: false, Limit: rule.Limit, RetryAfter: time.Second, Reset: time.Second, Source: "fail-closed"}
	default:
		// Approximate but bounded: no shared state is available, so each
		// instance enforces its share of the limit. The headers then
		// describe this instance's local view, not the global limit.
		d, _ := r.fallback.Allow(ctx, rule.Scaled(r.instances), subject)
		d.Source = "fallback"
		return d
	}
}
