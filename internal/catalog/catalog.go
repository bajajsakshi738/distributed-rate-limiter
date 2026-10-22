// Package catalog holds the current configuration (rules, plans, settings)
// that every request reads, and keeps it fresh.
//
// Changes flow like this: the dashboard writes to Postgres, then sets a new
// random value in config:version (rules and settings) or keys:version (API
// key revocations) in Redis. Every instance checks both every 2 seconds:
// a new config version reloads the snapshot from Postgres; a new keys version
// clears the API key caches. A full reload every 60 seconds is the backup for
// when Redis is down.
package catalog

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
)

type Plan struct {
	ID     int64
	Name   string
	RuleID int64
}

type Settings struct {
	Store       string // "memory" or "redis"; empty means use the instance's LIMITER_STORE
	FailureMode limiter.FailureMode
}

type Snapshot struct {
	Rules    map[int64]limiter.Rule
	Plans    map[int64]Plan
	Settings Settings
	LoadedAt time.Time
}

// RuleForPlan returns the rule attached to a plan.
func (s *Snapshot) RuleForPlan(planID int64) (limiter.Rule, Plan, bool) {
	p, ok := s.Plans[planID]
	if !ok {
		return limiter.Rule{}, Plan{}, false
	}
	r, ok := s.Rules[p.RuleID]
	return r, p, ok
}

// Loader reads the full configuration (implemented by the Postgres store).
type Loader interface {
	LoadSnapshot(ctx context.Context) (*Snapshot, error)
}

// Versions are opaque change markers. They are random values rather than
// counters: a counter (INCR) restarts from 1 when a Redis without persistence
// restarts, and an instance that last saw "1" would then miss a change.
type Versions struct {
	Config string
	Keys   string
}

// VersionSource returns the current change markers (implemented with Redis).
type VersionSource interface {
	Versions(ctx context.Context) (Versions, error)
}

type Watcher struct {
	loader     Loader
	versions   VersionSource
	current    atomic.Pointer[Snapshot]
	last       Versions
	poll       time.Duration
	fullReload time.Duration
	log        *slog.Logger

	mu           sync.Mutex
	onKeysChange []func()
	reloads      atomic.Int64
	reloadErr    atomic.Int64
}

func NewWatcher(loader Loader, versions VersionSource, log *slog.Logger) *Watcher {
	return &Watcher{
		loader:     loader,
		versions:   versions,
		poll:       2 * time.Second,
		fullReload: 60 * time.Second,
		log:        log,
	}
}

// SetIntervals is used by tests.
func (w *Watcher) SetIntervals(poll, full time.Duration) { w.poll, w.fullReload = poll, full }

// OnKeysChange registers a callback run when keys:version changes (used to
// clear the API key caches so revocations apply quickly). Rule and settings
// changes don't clear the caches: emptying them on every instance at once
// would send every in-flight key to Postgres at the same moment.
func (w *Watcher) OnKeysChange(f func()) {
	w.mu.Lock()
	w.onKeysChange = append(w.onKeysChange, f)
	w.mu.Unlock()
}

func (w *Watcher) Current() *Snapshot { return w.current.Load() }
func (w *Watcher) Reloads() int64     { return w.reloads.Load() }

// Set installs a snapshot directly (used by tests and the first load).
func (w *Watcher) Set(s *Snapshot) { w.current.Store(s) }

// LoadInitial retries until the first configuration is loaded or ctx ends.
func (w *Watcher) LoadInitial(ctx context.Context) error {
	backoff := 250 * time.Millisecond
	for {
		// Read the versions BEFORE loading. If they were read after, a change
		// committed in between would be marked as seen without being loaded.
		v, verr := w.versions.Versions(ctx)
		err := w.reload(ctx)
		if err == nil {
			if verr == nil {
				w.last = v
			}
			return nil
		}
		w.log.Warn("initial config load failed, retrying", "err", err)
		select {
		case <-ctx.Done():
			return err
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// Run polls for changes until ctx is done.
func (w *Watcher) Run(ctx context.Context) {
	pollT := time.NewTicker(w.poll)
	fullT := time.NewTicker(w.fullReload)
	defer pollT.Stop()
	defer fullT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-pollT.C:
			vctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			v, err := w.versions.Versions(vctx)
			cancel()
			if err != nil {
				continue // Redis down; the full reload covers us
			}
			if v.Config != w.last.Config {
				if err := w.reload(ctx); err == nil {
					w.last.Config = v.Config
				}
			}
			if v.Keys != w.last.Keys {
				w.last.Keys = v.Keys
				w.fireKeysChange()
			}
		case <-fullT.C:
			// Backup path. If Postgres is unreachable the old snapshot is kept.
			_ = w.reload(ctx)
		}
	}
}

func (w *Watcher) reload(ctx context.Context) error {
	lctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	s, err := w.loader.LoadSnapshot(lctx)
	if err != nil {
		w.reloadErr.Add(1)
		w.log.Warn("config reload failed, keeping previous config", "err", err)
		return err
	}
	w.current.Store(s)
	w.reloads.Add(1)
	return nil
}

func (w *Watcher) fireKeysChange() {
	w.mu.Lock()
	fs := append([]func(){}, w.onKeysChange...)
	w.mu.Unlock()
	for _, f := range fs {
		f()
	}
}
