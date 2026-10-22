package loadgen

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestSteadyRunCountsAndRate(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Error("missing auth header")
		}
		w.Header().Set("X-Instance", "api-x")
		if n.Add(1)%2 == 0 {
			w.WriteHeader(http.StatusTooManyRequests)
		}
	}))
	defer srv.Close()

	res, err := Run(context.Background(), Config{
		Target: srv.URL, Keys: []string{"a", "b"}, RPS: 200, Duration: 2 * time.Second, Pattern: PatternSteady,
	}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if res.Sent != 400 || res.Allowed+res.Rejected != 400 || res.Errors != 0 {
		t.Fatalf("sent=%d allowed=%d rejected=%d errors=%d", res.Sent, res.Allowed, res.Rejected, res.Errors)
	}
	if res.Allowed != 200 {
		t.Fatalf("allowed %d, want 200", res.Allowed)
	}
	// open loop: the run takes about the scheduled duration
	if res.Elapsed < 1900*time.Millisecond || res.Elapsed > 3*time.Second {
		t.Fatalf("elapsed %v", res.Elapsed)
	}
	if len(res.Buckets) < 2 || res.Buckets[0].Instance != "api-x" {
		t.Fatalf("buckets %+v", res.Buckets)
	}
	if res.P99 <= 0 || res.P50 > res.P99 {
		t.Fatalf("p50=%v p99=%v", res.P50, res.P99)
	}
}

func TestErrorsCounted(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	res, _ := Run(context.Background(), Config{Target: srv.URL, Keys: []string{"a"}, RPS: 20, Duration: time.Second, Pattern: PatternSteady}, srv.Client())
	if res.Errors != 20 || res.Allowed != 0 {
		t.Fatalf("errors=%d allowed=%d", res.Errors, res.Allowed)
	}
}

func TestBoundaryScheduleStraddlesSeconds(t *testing.T) {
	start := time.Unix(1_700_000_000, 300_000_000) // 300ms past a second
	shots := schedule(Config{Keys: []string{"a"}, RPS: 5, Duration: 5 * time.Second, Pattern: PatternBoundary}, start)
	if len(shots) == 0 || len(shots)%10 != 0 {
		t.Fatalf("shots %d", len(shots))
	}
	before, after := 0, 0
	for _, s := range shots[:10] {
		abs := start.Add(s.at)
		if abs.Nanosecond() > 900_000_000 {
			before++
		} else if abs.Nanosecond() < 100_000_000 {
			after++
		}
	}
	if before != 5 || after != 5 {
		t.Fatalf("first burst: %d before, %d after the boundary", before, after)
	}
}

func TestPeakInWindow(t *testing.T) {
	ms := func(v ...int) []time.Duration {
		out := make([]time.Duration, len(v))
		for i, x := range v {
			out[i] = time.Duration(x) * time.Millisecond
		}
		return out
	}
	if p := peakInWindow(ms(0, 100, 999, 1000, 1500, 1999), time.Second); p != 3 {
		t.Fatalf("peak %d, want 3", p)
	}
	if p := peakInWindow(nil, time.Second); p != 0 {
		t.Fatal(p)
	}
}

func TestHistogramQuantile(t *testing.T) {
	h := Histogram{
		Bounds: []float64{0.001, 0.002, 0.004, math.Inf(1)},
		Counts: []float64{50, 90, 100, 100},
	}
	p50, _ := h.Quantile(0.5)
	if p50 != time.Millisecond {
		t.Fatalf("p50 %v", p50)
	}
	p99, _ := h.Quantile(0.99)
	if p99 < 3700*time.Microsecond || p99 > 3900*time.Microsecond { // interpolated inside the 2-4ms bucket
		t.Fatalf("p99 %v", p99)
	}
	before := Histogram{Bounds: h.Bounds, Counts: []float64{50, 50, 50, 50}}
	d := h.Sub(before)
	if d.Total() != 50 {
		t.Fatalf("diff total %v", d.Total())
	}
	if _, ok := (Histogram{}).Quantile(0.5); ok {
		t.Fatal("empty histogram has no quantile")
	}
}

// Scrapes a real Prometheus registry over HTTP, the way the dashboard does.
func TestScrapeLimiterHistogram(t *testing.T) {
	reg := prometheus.NewRegistry()
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ratelimiter_limiter_duration_seconds", Buckets: []float64{0.0001, 0.0005, 0.001, 0.005},
	}, []string{"source"})
	reg.MustRegister(h)
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()

	before, err := ScrapeLimiterHistogram(context.Background(), srv.Client(), []string{srv.URL, srv.URL}, "")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 98; i++ {
		h.WithLabelValues("redis").Observe(0.0003)
	}
	h.WithLabelValues("memory").Observe(0.002)
	h.WithLabelValues("memory").Observe(0.002)
	after, err := ScrapeLimiterHistogram(context.Background(), srv.Client(), []string{srv.URL, srv.URL}, "")
	if err != nil {
		t.Fatal(err)
	}
	d := after.Sub(before)
	if d.Total() != 200 { // scraped twice, so every observation counts twice
		t.Fatalf("total %v, want 200", d.Total())
	}
	p50, _ := d.Quantile(0.5)
	p99, _ := d.Quantile(0.99)
	if p50 < 100*time.Microsecond || p50 > 500*time.Microsecond {
		t.Fatalf("p50 %v should fall in the 0.1-0.5ms bucket", p50)
	}
	if p99 < time.Millisecond || p99 > 5*time.Millisecond {
		t.Fatalf("p99 %v should fall in the 1-5ms bucket", p99)
	}
}

func TestScrapeFiltersByStore(t *testing.T) {
	reg := prometheus.NewRegistry()
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ratelimiter_limiter_duration_seconds", Buckets: []float64{0.0001, 0.001},
	}, []string{"source"})
	reg.MustRegister(h)
	srv := httptest.NewServer(promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	defer srv.Close()
	h.WithLabelValues("redis").Observe(0.0005)
	h.WithLabelValues("memory").Observe(0.00001)
	h.WithLabelValues("memory").Observe(0.00001)
	got, err := ScrapeLimiterHistogram(context.Background(), srv.Client(), []string{srv.URL}, "redis")
	if err != nil || got.Total() != 1 {
		t.Fatalf("redis-only total %v err %v", got.Total(), err)
	}
}
