// Package loadgen is an open-loop load generator.
//
// Open-loop means requests are sent on a fixed schedule, whether or not
// earlier requests have finished. A closed-loop generator (send, wait for the
// reply, send again) slows down exactly when the server slows down, so it
// under-reports latency ("coordinated omission"). Here, latency is measured
// from when a request was *scheduled*, so any delay in sending it counts too.
package loadgen

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

const (
	PatternSteady   = "steady"
	PatternBoundary = "boundary"
)

type Config struct {
	Target      string        // full URL, e.g. http://nginx/api/data
	Keys        []string      // one per simulated client
	RPS         int           // steady: total requests per second; boundary: requests per burst, per client
	Duration    time.Duration // how long to run
	Pattern     string
	Timeout     time.Duration // per request
	MaxInFlight int           // safety cap on concurrent requests
}

func (c Config) Validate() error {
	if c.Target == "" || len(c.Keys) == 0 {
		return errors.New("target and keys are required")
	}
	if c.RPS < 1 || c.Duration < time.Second {
		return errors.New("rps must be >= 1 and duration >= 1s")
	}
	if c.Pattern != PatternSteady && c.Pattern != PatternBoundary {
		return errors.New("pattern must be steady or boundary")
	}
	return nil
}

// Bucket is one second of results as seen by the client, per instance.
type Bucket struct {
	Second   int
	Instance string
	Allowed  int
	Rejected int
	Errors   int
}

type Result struct {
	Sent, Allowed, Rejected, Errors int64
	P50, P99                        time.Duration
	// Peak1sAllowed is the most requests any single client got through in
	// any 1-second span (sliding, not aligned). This is where the fixed
	// window's boundary burst shows up as ~2x the limit.
	Peak1sAllowed int
	Buckets       []Bucket
	Elapsed       time.Duration
	// Sources counts allowed responses by X-RateLimit-Source (redis, memory,
	// fallback, ...), so tests can check which limiter actually decided.
	Sources map[string]int64
}

type shot struct {
	at  time.Duration // offset from start
	key int
}

// schedule builds the full list of send times up front.
func schedule(cfg Config, start time.Time) []shot {
	var shots []shot
	switch cfg.Pattern {
	case PatternBoundary:
		// Every 2 seconds, each client sends a burst just before a whole
		// second and another just after it. Fixed windows reset at whole
		// seconds, so both bursts land in different windows.
		const spread = 35 * time.Millisecond
		first := start.Truncate(time.Second).Add(2 * time.Second)
		for b := first; b.Sub(start) < cfg.Duration; b = b.Add(2 * time.Second) {
			for k := range cfg.Keys {
				for j := 0; j < cfg.RPS; j++ {
					step := time.Duration(j) * spread / time.Duration(cfg.RPS)
					shots = append(shots,
						shot{at: b.Add(-40 * time.Millisecond).Add(step).Sub(start), key: k},
						shot{at: b.Add(5 * time.Millisecond).Add(step).Sub(start), key: k})
				}
			}
		}
		sort.Slice(shots, func(i, j int) bool { return shots[i].at < shots[j].at })
	default:
		total := int(cfg.Duration.Seconds() * float64(cfg.RPS))
		interval := time.Second / time.Duration(cfg.RPS)
		shots = make([]shot, total)
		for i := range shots {
			shots[i] = shot{at: time.Duration(i) * interval, key: i % len(cfg.Keys)}
		}
	}
	return shots
}

type outcome struct {
	sec      int
	key      int
	sentAt   time.Duration
	latency  time.Duration
	instance string
	source   string
	status   int // 0 = transport error
}

// Run executes the load test. It returns early (with partial results) if ctx is cancelled.
func Run(ctx context.Context, cfg Config, client *http.Client) (Result, error) {
	if err := cfg.Validate(); err != nil {
		return Result{}, err
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = 4000
	}
	start := time.Now()
	shots := schedule(cfg, start)
	outcomes := make([]outcome, len(shots))
	sem := make(chan struct{}, cfg.MaxInFlight)
	var wg sync.WaitGroup

	sent := 0
	for i, s := range shots {
		if wait := time.Until(start.Add(s.at)); wait > 0 {
			select {
			case <-ctx.Done():
				goto done
			case <-time.After(wait):
			}
		}
		sent++
		select {
		case sem <- struct{}{}:
		default:
			// Too many requests in flight: the generator itself is overloaded.
			// Count it as an error instead of waiting (waiting would make this closed-loop).
			outcomes[i] = outcome{sec: int(s.at / time.Second), key: s.key, sentAt: s.at, instance: "generator-overloaded"}
			continue
		}
		wg.Add(1)
		go func(i int, s shot) {
			defer wg.Done()
			defer func() { <-sem }()
			outcomes[i] = send(ctx, client, cfg, s, start)
		}(i, s)
	}
done:
	wg.Wait()
	return summarize(outcomes[:sent], time.Since(start)), nil
}

func send(ctx context.Context, client *http.Client, cfg Config, s shot, start time.Time) outcome {
	o := outcome{sec: int(s.at / time.Second), key: s.key, sentAt: time.Since(start)}
	rctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodGet, cfg.Target, nil)
	if err != nil {
		o.instance = "error"
		return o
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Keys[s.key])
	resp, err := client.Do(req)
	// Latency is measured from the scheduled time, not the actual send time.
	o.latency = time.Since(start.Add(s.at))
	if err != nil {
		o.instance = "error"
		return o
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	o.status = resp.StatusCode
	o.instance = resp.Header.Get("X-Instance")
	o.source = resp.Header.Get("X-RateLimit-Source")
	if o.instance == "" {
		o.instance = "unknown"
	}
	return o
}

func summarize(outs []outcome, elapsed time.Duration) Result {
	res := Result{Sent: int64(len(outs)), Elapsed: elapsed, Sources: map[string]int64{}}
	type bk struct {
		sec  int
		inst string
	}
	buckets := map[bk]*Bucket{}
	allowedAt := map[int][]time.Duration{}
	lat := make([]time.Duration, 0, len(outs))

	for _, o := range outs {
		b := buckets[bk{o.sec, o.instance}]
		if b == nil {
			b = &Bucket{Second: o.sec, Instance: o.instance}
			buckets[bk{o.sec, o.instance}] = b
		}
		switch o.status {
		case http.StatusOK:
			res.Allowed++
			b.Allowed++
			res.Sources[o.source]++
			allowedAt[o.key] = append(allowedAt[o.key], o.sentAt)
			lat = append(lat, o.latency)
		case http.StatusTooManyRequests:
			res.Rejected++
			b.Rejected++
			lat = append(lat, o.latency)
		default:
			res.Errors++
			b.Errors++
		}
	}
	for _, b := range buckets {
		res.Buckets = append(res.Buckets, *b)
	}
	sort.Slice(res.Buckets, func(i, j int) bool {
		if res.Buckets[i].Second != res.Buckets[j].Second {
			return res.Buckets[i].Second < res.Buckets[j].Second
		}
		return res.Buckets[i].Instance < res.Buckets[j].Instance
	})
	if len(lat) > 0 {
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		res.P50 = lat[percentileIndex(len(lat), 0.50)]
		res.P99 = lat[percentileIndex(len(lat), 0.99)]
	}
	for _, ts := range allowedAt {
		if p := peakInWindow(ts, time.Second); p > res.Peak1sAllowed {
			res.Peak1sAllowed = p
		}
	}
	return res
}

func percentileIndex(n int, q float64) int {
	i := int(float64(n)*q+0.5) - 1
	return min(max(i, 0), n-1)
}

// peakInWindow returns the largest number of timestamps inside any window of
// the given length (two-pointer sweep over sorted times).
func peakInWindow(ts []time.Duration, window time.Duration) int {
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
	best, lo := 0, 0
	for hi := range ts {
		for ts[hi]-ts[lo] >= window {
			lo++
		}
		best = max(best, hi-lo+1)
	}
	return best
}
