package dashboard

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/catalog"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/loadgen"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/store"
)

// runner owns the single load test that may run at a time.
type runner struct {
	s *Server

	mu      sync.Mutex
	runID   int64
	started time.Time
	marked  bool
	rule    limiter.Rule // the running test's limit, for the live "got through" box
	clients int
}

// progress reports the running test's limit, client count and start time.
func (r *runner) progress() (rule limiter.Rule, clients int, started time.Time, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rule, r.clients, r.started, r.runID > 0
}

func (r *runner) active() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runID != 0
}

type loadRequest struct {
	Plan     string
	Clients  int
	RPS      int
	Duration time.Duration
	Pattern  string
}

var errBusy = errors.New("a load test is already running")

func (r *runner) start(ctx context.Context, req loadRequest, plan catalog.Plan, rule limiter.Rule, storeName string) (int64, error) {
	r.mu.Lock()
	if r.runID != 0 {
		r.mu.Unlock()
		return 0, errBusy
	}
	r.runID = -1 // reserve
	r.mu.Unlock()

	st := r.s.opt.Store
	run := store.Run{
		PlanName: plan.Name, Clients: req.Clients, RPS: req.RPS, DurationS: int(req.Duration.Seconds()),
		Pattern: req.Pattern, Store: storeName, Algorithm: string(rule.Algorithm),
	}
	if rule.Algorithm == limiter.FixedWindow {
		run.LimitDesc = fmt.Sprintf("%d per %s window, per client", rule.Limit, rule.Window)
	} else {
		run.LimitDesc = fmt.Sprintf("%d per %s, burst %d, per client", rule.Limit, rule.Window, rule.Capacity())
	}
	id, err := st.CreateRun(ctx, run)
	if err != nil {
		r.release()
		return 0, err
	}
	run.ID = id
	keys := make([]string, req.Clients)
	for i := range keys {
		if keys[i], err = r.s.opt.Signer.Generate(); err != nil {
			break
		}
	}
	if err == nil {
		err = st.CreateRunKeys(ctx, keys, plan.ID, id)
	}
	if err != nil {
		run.Status = "failed"
		st.FinishRun(ctx, run, nil)
		r.release()
		return 0, err
	}

	r.mu.Lock()
	r.runID, r.started, r.marked = id, time.Now(), false
	r.rule, r.clients = rule, req.Clients
	r.mu.Unlock()

	go r.execute(run, keys, req, rule)
	return id, nil
}

func (r *runner) release() {
	r.mu.Lock()
	r.runID = 0
	r.mu.Unlock()
}

func (r *runner) execute(run store.Run, keys []string, req loadRequest, rule limiter.Rule) {
	defer r.release()
	log := r.s.opt.Log.With("run", run.ID)
	// Detached from the HTTP request that started it; bounded by the run length.
	ctx, cancel := context.WithTimeout(context.Background(), req.Duration+2*time.Minute)
	defer cancel()

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        2000,
			MaxIdleConnsPerHost: 2000,
			IdleConnTimeout:     30 * time.Second,
		},
	}
	defer client.CloseIdleConnections()

	metricsURLs := make([]string, len(r.s.opt.InstanceURLs))
	for i, u := range r.s.opt.InstanceURLs {
		metricsURLs[i] = u + "/metrics"
	}
	// Server-side overhead of the Redis limiter call (memory mode costs
	// microseconds and isn't the interesting number).
	before, beforeErr := loadgen.ScrapeLimiterHistogram(ctx, client, metricsURLs, "redis")
	hits0, lookups0 := cacheCounts(r.s.poller.Snapshot())

	res, err := loadgen.Run(ctx, loadgen.Config{
		Target: r.s.opt.LoadTarget, Keys: keys, RPS: req.RPS, Duration: req.Duration, Pattern: req.Pattern,
	}, client)
	run.Status = "done"
	if err != nil {
		log.Error("load test failed", "err", err)
		run.Status = "failed"
	}

	after, afterErr := loadgen.ScrapeLimiterHistogram(ctx, client, metricsURLs, "redis")
	if beforeErr == nil && afterErr == nil {
		if q, ok := after.Sub(before).Quantile(0.99); ok {
			v := int(q.Microseconds())
			run.LimiterP99us = &v
		}
	} else {
		log.Warn("could not scrape limiter latency", "before", beforeErr, "after", afterErr)
	}

	run.Sent, run.Allowed, run.Rejected, run.Errors = res.Sent, res.Allowed, res.Rejected, res.Errors
	if most := float64(req.Clients) * rule.MaxAllowed(res.Elapsed); most > 0 && res.Elapsed > 0 {
		ratio := float64(res.Allowed) / most
		run.LimitRatio = &ratio
	}
	// The poller refreshes once a second; wait for a poll that includes the
	// end of the run, then compare the key cache counters with the start.
	select {
	case <-time.After(1200 * time.Millisecond):
	case <-ctx.Done():
	}
	if hits1, lookups1 := cacheCounts(r.s.poller.Snapshot()); lookups1 > lookups0 && hits1 >= hits0 {
		pct := 100 * float64(hits1-hits0) / float64(lookups1-lookups0)
		run.CacheHitPct = &pct
	}
	p50, p99, peak := int(res.P50.Microseconds()), int(res.P99.Microseconds()), res.Peak1sAllowed
	run.P50us, run.P99us, run.Peak1sAllowed = &p50, &p99, &peak

	secs := make([]store.RunSecond, len(res.Buckets))
	for i, b := range res.Buckets {
		secs[i] = store.RunSecond{Second: b.Second, Instance: b.Instance, Allowed: b.Allowed, Rejected: b.Rejected, Errors: b.Errors}
	}
	saveCtx, saveCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer saveCancel()
	if err := r.s.opt.Store.FinishRun(saveCtx, run, secs); err != nil {
		log.Error("saving run failed", "err", err)
	}
	if err := r.s.opt.Store.RevokeRunKeys(saveCtx, run.ID); err != nil {
		log.Error("revoking run keys failed", "err", err)
	}
	if r.s.opt.Bumper != nil {
		r.s.opt.Bumper.BumpKeys(saveCtx) // revoked run keys: clear key caches on all instances
	}
	r.s.invalidateReplay()
	log.Info("load test finished", "sent", res.Sent, "allowed", res.Allowed, "rejected", res.Rejected,
		"errors", res.Errors, "p99", res.P99, "peak_1s_allowed", res.Peak1sAllowed)
}

// mark records the first notable event of the current run as its marker.
func (r *runner) mark(label string) {
	r.mu.Lock()
	id, started, marked := r.runID, r.started, r.marked
	if id > 0 && !marked {
		r.marked = true
	}
	r.mu.Unlock()
	if id <= 0 || marked {
		return
	}
	sec := int(time.Since(started).Seconds())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.s.opt.Store.SetRunMarker(ctx, id, sec, label); err != nil {
		r.s.opt.Log.Warn("could not save run marker", "err", err)
	}
}

// storeSwitched records a memory/redis switch on the running run.
func (r *runner) storeSwitched(newStore string) {
	r.mu.Lock()
	id := r.runID
	r.mu.Unlock()
	if id <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.s.opt.Store.NoteStoreSwitch(ctx, id, newStore); err != nil {
		r.s.opt.Log.Warn("could not record store switch", "err", err)
	}
	r.mark("switched to " + newStore)
}

// observe is called after every poll: if an instance reports its Redis
// breaker open during a run, that moment becomes the run's marker.
func (r *runner) observe(insts []instanceState) {
	if !r.active() {
		return
	}
	for _, in := range insts {
		if in.OK && in.Stats.Store == "redis" && in.Stats.Breaker != "" && in.Stats.Breaker != string(limiter.BreakerClosed) {
			r.mark("Redis unreachable (" + strings.ToLower(in.Stats.FailureMode) + " mode)")
			return
		}
	}
}
