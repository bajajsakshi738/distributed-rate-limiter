// Package stats keeps per-instance, in-memory traffic counters: allowed and
// rejected requests per second for the last 2 minutes, and the last 100
// requests. The dashboard polls each instance for them.
//
// These deliberately live in memory, not Redis: when Redis is killed for the
// failure demo, the dashboard must keep showing what's happening.
package stats

import (
	"sync"
	"time"
)

const (
	WindowSeconds = 120
	RecentSize    = 100
)

type Second struct {
	Unix     int64 `json:"unix"`
	Allowed  int   `json:"allowed"`
	Rejected int   `json:"rejected"`
	Degraded int   `json:"degraded"` // decided without Redis (fallback/open/closed)
}

type Request struct {
	At        time.Time `json:"at"`
	Key       string    `json:"key"` // display prefix only, never the full key
	Plan      string    `json:"plan"`
	Status    int       `json:"status"`
	Remaining int       `json:"remaining"`
	Source    string    `json:"source"`
	LatencyUs int64     `json:"latency_us"`
}

type Recorder struct {
	mu      sync.Mutex
	seconds [WindowSeconds]Second
	recent  [RecentSize]Request
	next    int
	count   int
}

func NewRecorder() *Recorder { return &Recorder{} }

func (r *Recorder) Record(req Request, allowed, degraded bool) {
	sec := req.At.Unix()
	r.mu.Lock()
	defer r.mu.Unlock()
	s := &r.seconds[sec%WindowSeconds]
	if s.Unix != sec {
		*s = Second{Unix: sec}
	}
	if allowed {
		s.Allowed++
	} else {
		s.Rejected++
	}
	if degraded {
		s.Degraded++
	}
	r.recent[r.next] = req
	r.next = (r.next + 1) % RecentSize
	if r.count < RecentSize {
		r.count++
	}
}

// Seconds returns the last n complete-or-current seconds up to now, oldest
// first, with zeros for seconds that had no traffic.
func (r *Recorder) Seconds(now time.Time, n int) []Second {
	if n > WindowSeconds {
		n = WindowSeconds
	}
	cur := now.Unix()
	out := make([]Second, n)
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < n; i++ {
		sec := cur - int64(n-1-i)
		s := r.seconds[sec%WindowSeconds]
		if s.Unix != sec {
			s = Second{Unix: sec}
		}
		out[i] = s
	}
	return out
}

// Recent returns the most recent requests, newest first.
func (r *Recorder) Recent() []Request {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Request, 0, r.count)
	for i := 1; i <= r.count; i++ {
		out = append(out, r.recent[(r.next-i+RecentSize)%RecentSize])
	}
	return out
}
