package limiter

import (
	"context"
	"hash/maphash"
	"math"
	"strconv"
	"sync"
	"time"
)

const shardCount = 32

// Memory keeps limiter state inside one process. With several instances
// behind a load balancer, each one keeps its own counts, which is exactly
// the bug this project demonstrates: a 10 req/s limit becomes ~30 req/s
// across 3 instances.
type Memory struct {
	now    func() time.Time
	seed   maphash.Seed
	shards [shardCount]memShard
}

type memShard struct {
	mu sync.Mutex
	m  map[string]*memState
}

type memState struct {
	// token bucket
	tokens float64
	lastMs int64
	// fixed window
	window int64
	count  int

	expiresMs int64
}

func NewMemory() *Memory { return NewMemoryWithClock(time.Now) }

// NewMemoryWithClock lets tests control time.
func NewMemoryWithClock(now func() time.Time) *Memory {
	m := &Memory{now: now, seed: maphash.MakeSeed()}
	for i := range m.shards {
		m.shards[i].m = make(map[string]*memState)
	}
	return m
}

func (m *Memory) shard(key string) *memShard {
	return &m.shards[maphash.String(m.seed, key)%shardCount]
}

func (m *Memory) Allow(_ context.Context, rule Rule, subject string) (Decision, error) {
	nowMs := m.now().UnixMilli()
	if rule.Algorithm == FixedWindow {
		return m.fixedWindow(rule, subject, nowMs), nil
	}
	return m.tokenBucket(rule, subject, nowMs), nil
}

// The math below mirrors scripts/token_bucket.lua exactly, so memory and
// Redis modes behave the same apart from where the state lives.
func (m *Memory) tokenBucket(rule Rule, subject string, nowMs int64) Decision {
	key := "tb:" + strconv.FormatInt(rule.ID, 10) + ":" + subject
	rate := rule.RatePerMs()
	capacity := float64(rule.Capacity())

	sh := m.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	st, ok := sh.m[key]
	if !ok || st.expiresMs <= nowMs {
		st = &memState{tokens: capacity, lastMs: nowMs}
		sh.m[key] = st
	}
	if nowMs > st.lastMs {
		st.tokens += float64(nowMs-st.lastMs) * rate
		st.lastMs = nowMs
	}
	// Caps refill, and also clamps an existing bucket when a rule's burst is lowered.
	if st.tokens > capacity {
		st.tokens = capacity
	}

	d := Decision{Limit: rule.Limit, Source: "memory"}
	if st.tokens >= 1 {
		st.tokens--
		d.Allowed = true
	} else {
		d.RetryAfter = msDur(math.Ceil((1 - st.tokens) / rate))
	}
	fullMs := math.Ceil((capacity - st.tokens) / rate)
	st.expiresMs = nowMs + int64(fullMs) + 1000
	d.Remaining = int(math.Floor(st.tokens))
	d.Reset = msDur(fullMs)
	return d
}

func (m *Memory) fixedWindow(rule Rule, subject string, nowMs int64) Decision {
	winMs := rule.Window.Milliseconds()
	key := "fw:" + strconv.FormatInt(rule.ID, 10) + ":" + strconv.FormatInt(winMs, 10) + ":" + subject
	cur := nowMs / winMs
	resetMs := (cur+1)*winMs - nowMs

	sh := m.shard(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	st, ok := sh.m[key]
	if !ok || st.window != cur {
		st = &memState{window: cur}
		sh.m[key] = st
	}
	st.expiresMs = nowMs + resetMs + 1000

	d := Decision{Limit: rule.Limit, Reset: msDur(float64(resetMs)), Source: "memory"}
	if st.count >= rule.Limit {
		d.RetryAfter = d.Reset
		return d
	}
	st.count++
	d.Allowed = true
	d.Remaining = rule.Limit - st.count
	return d
}

// Sweep deletes expired entries so memory doesn't grow forever when many
// different clients appear (for example during load tests).
func (m *Memory) Sweep() int {
	nowMs := m.now().UnixMilli()
	removed := 0
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.Lock()
		for k, st := range sh.m {
			if st.expiresMs <= nowMs {
				delete(sh.m, k)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

// Len returns the number of tracked entries (for tests and metrics).
func (m *Memory) Len() int {
	n := 0
	for i := range m.shards {
		m.shards[i].mu.Lock()
		n += len(m.shards[i].m)
		m.shards[i].mu.Unlock()
	}
	return n
}

// Reset drops all state (used when switching stores so old counts don't linger).
func (m *Memory) Reset() {
	for i := range m.shards {
		m.shards[i].mu.Lock()
		m.shards[i].m = make(map[string]*memState)
		m.shards[i].mu.Unlock()
	}
}

// RunSweeper calls Sweep every interval until ctx is done.
func (m *Memory) RunSweeper(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Sweep()
		}
	}
}

func msDur(ms float64) time.Duration {
	if ms < 0 {
		ms = 0
	}
	return time.Duration(ms) * time.Millisecond
}
