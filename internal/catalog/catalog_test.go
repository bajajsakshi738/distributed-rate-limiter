package catalog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
)

type fakeLoader struct {
	limit atomic.Int64
	fail  atomic.Bool
	// onLoad runs inside LoadSnapshot (used to simulate a change racing a reload).
	onLoad func()
}

func (f *fakeLoader) LoadSnapshot(context.Context) (*Snapshot, error) {
	if f.fail.Load() {
		return nil, errors.New("postgres down")
	}
	snap := &Snapshot{
		Rules: map[int64]limiter.Rule{1: {ID: 1, Algorithm: limiter.TokenBucket, Limit: int(f.limit.Load()), Window: time.Second}},
		Plans: map[int64]Plan{1: {ID: 1, Name: "free", RuleID: 1}},
	}
	if f.onLoad != nil {
		f.onLoad()
	}
	return snap, nil
}

type fakeVersion struct {
	mu   sync.Mutex
	v    Versions
	fail bool
}

func (f *fakeVersion) set(cfg, keys string) {
	f.mu.Lock()
	f.v = Versions{Config: cfg, Keys: keys}
	f.mu.Unlock()
}

func (f *fakeVersion) setFail(b bool) {
	f.mu.Lock()
	f.fail = b
	f.mu.Unlock()
}

func (f *fakeVersion) Versions(context.Context) (Versions, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return Versions{}, errors.New("redis down")
	}
	return f.v, nil
}

func limitOf(w *Watcher) int {
	r, _, _ := w.Current().RuleForPlan(1)
	return r.Limit
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func newWatcher(l *fakeLoader, v *fakeVersion, poll, full time.Duration) *Watcher {
	w := NewWatcher(l, v, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.SetIntervals(poll, full)
	return w
}

func TestConfigChangeReloadsWithoutPurgingKeys(t *testing.T) {
	l := &fakeLoader{}
	l.limit.Store(10)
	v := &fakeVersion{}
	v.set("a", "a")
	w := newWatcher(l, v, 10*time.Millisecond, time.Hour)
	var purges atomic.Int64
	w.OnKeysChange(func() { purges.Add(1) })
	if err := w.LoadInitial(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	l.limit.Store(20)
	time.Sleep(50 * time.Millisecond)
	if limitOf(w) != 10 {
		t.Fatal("must not reload while the version is unchanged")
	}
	v.set("b", "a")
	waitFor(t, func() bool { return limitOf(w) == 20 })
	time.Sleep(30 * time.Millisecond)
	if purges.Load() != 0 {
		t.Fatal("a rule/settings change must not clear the key caches")
	}

	v.set("b", "b")
	waitFor(t, func() bool { return purges.Load() == 1 })
}

func TestFullReloadWhenRedisDown(t *testing.T) {
	l := &fakeLoader{}
	l.limit.Store(10)
	v := &fakeVersion{}
	w := newWatcher(l, v, 10*time.Millisecond, 30*time.Millisecond)
	w.LoadInitial(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	v.setFail(true)
	l.limit.Store(30)
	waitFor(t, func() bool { return limitOf(w) == 30 })
}

func TestKeepsOldConfigWhenPostgresDown(t *testing.T) {
	l := &fakeLoader{}
	l.limit.Store(10)
	v := &fakeVersion{}
	w := newWatcher(l, v, 10*time.Millisecond, 20*time.Millisecond)
	w.LoadInitial(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	l.fail.Store(true)
	v.set("x", "")
	time.Sleep(100 * time.Millisecond)
	if w.Current() == nil || limitOf(w) != 10 {
		t.Fatal("old config must be kept when a reload fails")
	}
}

// A change committed while the initial load is running must not be marked
// as seen: the version is read before loading, so the next poll reloads.
func TestChangeDuringInitialLoadIsNotLost(t *testing.T) {
	l := &fakeLoader{}
	l.limit.Store(10)
	v := &fakeVersion{}
	v.set("a", "")
	l.onLoad = func() {
		l.onLoad = nil
		l.limit.Store(50) // committed to Postgres right after the snapshot was read...
		v.set("b", "")    // ...and announced
	}
	w := newWatcher(l, v, 10*time.Millisecond, time.Hour)
	w.LoadInitial(context.Background())
	if limitOf(w) != 10 {
		t.Fatalf("initial snapshot %d", limitOf(w))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)
	waitFor(t, func() bool { return limitOf(w) == 50 })
}
