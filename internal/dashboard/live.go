package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/api"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/stats"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/store"
)

// Poller fetches /internal/stats from every API instance once a second.
// Viewers never trigger these calls: however many people have the page open,
// the instances are polled at the same fixed rate.
type Poller struct {
	urls   []string
	client *http.Client

	mu     sync.RWMutex
	latest map[string]instanceState // by URL
}

type instanceState struct {
	URL   string
	OK    bool
	Err   string
	Stats api.StatsResponse
	At    time.Time
}

func NewPoller(urls []string) *Poller {
	return &Poller{
		urls:   urls,
		client: &http.Client{Timeout: 800 * time.Millisecond},
		latest: map[string]instanceState{},
	}
}

func (p *Poller) Run(ctx context.Context, onPoll func([]instanceState)) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		p.pollOnce(ctx)
		if onPoll != nil {
			onPoll(p.Snapshot())
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context) {
	var wg sync.WaitGroup
	for _, u := range p.urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			st := instanceState{URL: u, At: time.Now()}
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u+"/internal/stats", nil)
			resp, err := p.client.Do(req)
			if err == nil {
				if resp.StatusCode == http.StatusOK {
					err = json.NewDecoder(resp.Body).Decode(&st.Stats)
				} else {
					err = fmt.Errorf("status %d", resp.StatusCode)
				}
				resp.Body.Close()
			}
			st.OK = err == nil
			if err != nil {
				st.Err = "unreachable"
				p.mu.RLock()
				st.Stats.Instance = p.latest[u].Stats.Instance // keep the last known name
				p.mu.RUnlock()
			}
			p.mu.Lock()
			p.latest[u] = st
			p.mu.Unlock()
		}(u)
	}
	wg.Wait()
}

func (p *Poller) Snapshot() []instanceState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]instanceState, 0, len(p.urls))
	for _, u := range p.urls {
		st, ok := p.latest[u]
		if !ok {
			st = instanceState{URL: u, Err: "waiting"}
		}
		out = append(out, st)
	}
	return out
}

// ---- view model for the live partial ----

type InstanceRow struct {
	Name        string
	OK          bool
	Status      string
	Store       string
	FailureMode string
	Breaker     string
	Allowed     int // last complete second
	Rejected    int
	Degraded    int
}

type LiveView struct {
	Mode         string // live | replay | idle
	Banner       string
	Chart        templateHTML
	AllowedNow   int
	RejectedNow  int
	Allowed60    int
	Rejected60   int
	CacheHitRate string
	Instances    []InstanceRow
	Recent       []RecentRow
	Run          *store.Run
	ReplayClock  string
	RunActive    bool
	// For the one-line status in the simple view.
	Store       string // what the servers are using now: redis or memory
	RedisDown   bool   // a server's Redis circuit breaker is open
	FailureMode string
	Ratio       string // "got through" box: allowed ÷ the most the limit permits, e.g. "0.97×"
	SinceStart  bool   // a test is running: Allowed60/Rejected60 count from its start
}

type RecentRow struct {
	Time     string
	Instance string
	stats.Request
}

const liveSeconds = 60

// buildLive aggregates the instances' last 60 complete seconds.
func buildLive(insts []instanceState, now time.Time) (points []Point, rows []InstanceRow, recent []RecentRow, hitRate string) {
	end := lastCompleteSecond(insts, now)
	points = make([]Point, liveSeconds)
	for _, in := range insts {
		row := InstanceRow{Name: in.Stats.Instance, OK: in.OK, Status: "up", Store: in.Stats.Store,
			FailureMode: in.Stats.FailureMode, Breaker: in.Stats.Breaker}
		if row.Name == "" {
			row.Name = in.URL
		}
		if !in.OK {
			row.Status = in.Err
		}
		if in.OK {
			for _, s := range in.Stats.Seconds {
				idx := int(s.Unix - (end - liveSeconds + 1))
				if idx >= 0 && idx < liveSeconds {
					points[idx].Allowed += s.Allowed
					points[idx].Rejected += s.Rejected
				}
				if s.Unix == end {
					row.Allowed, row.Rejected, row.Degraded = s.Allowed, s.Rejected, s.Degraded
				}
			}
			for _, r := range in.Stats.Recent {
				recent = append(recent, RecentRow{Time: r.At.Format("15:04:05.000"), Instance: row.Name, Request: r})
			}
		}
		rows = append(rows, row)
	}
	sort.Slice(recent, func(i, j int) bool { return recent[i].At.After(recent[j].At) })
	if len(recent) > 20 {
		recent = recent[:20]
	}
	hits, lookups := cacheCounts(insts)
	hitRate = "–"
	if lookups > 0 {
		hitRate = formatPct(100 * float64(hits) / float64(lookups))
	}
	return points, rows, recent, hitRate
}

func hasRecentTraffic(points []Point, seconds int) bool {
	for _, p := range points[max(0, len(points)-seconds):] {
		if p.Allowed+p.Rejected > 0 {
			return true
		}
	}
	return false
}

// ---- replay ----

// Replay is a recorded run, aggregated per second across instances.
type Replay struct {
	Run    store.Run
	Points []Point
}

const replayPause = 5 // seconds the finished chart stays up before looping

// Frame returns which second of the replay to show. It depends only on the
// clock, so every viewer sees the same moment and the server keeps no
// per-viewer state.
func (r *Replay) Frame(now time.Time) int {
	n := len(r.Points)
	if n == 0 {
		return 0
	}
	return int(now.Unix() % int64(n+replayPause))
}

func NewReplay(run store.Run, secs []store.RunSecond) *Replay {
	n := run.DurationS
	for _, s := range secs {
		n = max(n, s.Second+1)
	}
	pts := make([]Point, n)
	for _, s := range secs {
		if s.Second >= 0 && s.Second < n {
			pts[s.Second].Allowed += s.Allowed
			pts[s.Second].Rejected += s.Rejected
		}
	}
	return &Replay{Run: run, Points: pts}
}

// cacheCounts adds up API key lookups across the instances that answered:
// how many were served from the in-memory cache, and how many there were.
func cacheCounts(insts []instanceState) (hits, lookups int64) {
	for _, in := range insts {
		if !in.OK {
			continue
		}
		c := in.Stats.Cache
		hits += c.Hits + c.NegativeHits
		lookups += c.Hits + c.NegativeHits + c.Misses
	}
	return hits, lookups
}

func formatRatio(f float64) string { return strconv.FormatFloat(f, 'f', 2, 64) + "×" }
func formatPct(f float64) string   { return strconv.FormatFloat(f, 'f', 1, 64) + "%" }

// lastCompleteSecond is the newest second (Unix time) that was already over
// when every instance was last polled. Using "now" instead would show a
// half-counted second. The live chart's last point is this second.
func lastCompleteSecond(insts []instanceState, now time.Time) int64 {
	polled := now
	for _, in := range insts {
		if in.OK && in.At.Before(polled) {
			polled = in.At
		}
	}
	return polled.Unix() - 1
}

// comma formats a count with thousands separators: 12345 -> "12,345".
func comma(n int) string {
	s := strconv.Itoa(n)
	if n < 0 {
		return "-" + comma(-n)
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
