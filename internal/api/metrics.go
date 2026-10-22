package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Metrics struct {
	Registry        *prometheus.Registry
	Requests        *prometheus.CounterVec
	LimiterDuration *prometheus.HistogramVec
	RequestDuration prometheus.Histogram
	KeyCache        *prometheus.CounterVec
	BreakerOpen     prometheus.Gauge
}

// LimiterBuckets are fine-grained because the limiter call is expected to
// take well under a millisecond.
var LimiterBuckets = []float64{
	0.00005, 0.0001, 0.00025, 0.0005, 0.00075, 0.001, 0.0015, 0.0025, 0.005, 0.01, 0.015, 0.025, 0.05,
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimiter_requests_total",
			Help: "Requests that reached the limiter, by decision and by what made the decision.",
		}, []string{"decision", "source"}),
		LimiterDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ratelimiter_limiter_duration_seconds",
			Help:    "Time spent in the limiter call only (the overhead the limiter adds), by what made the decision: redis, memory, fallback, fail-open, fail-closed.",
			Buckets: LimiterBuckets,
		}, []string{"source"}),
		RequestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "ratelimiter_request_duration_seconds",
			Help:    "Total time to handle /api/data inside the instance.",
			Buckets: LimiterBuckets,
		}),
		KeyCache: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimiter_key_lookups_total",
			Help: "API key lookups by result: hit, negative_hit, miss (went to Postgres), malformed (rejected by format check).",
		}, []string{"result"}),
		BreakerOpen: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "ratelimiter_redis_breaker_open",
			Help: "1 when the Redis circuit breaker is open or half-open.",
		}),
	}
	reg.MustRegister(m.Requests, m.LimiterDuration, m.RequestDuration, m.KeyCache, m.BreakerOpen,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}
