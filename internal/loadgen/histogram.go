package loadgen

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// Histogram is a cumulative Prometheus histogram summed across instances.
type Histogram struct {
	Bounds []float64 // upper bounds in seconds, ascending, last is +Inf
	Counts []float64 // cumulative counts
}

const limiterMetric = "ratelimiter_limiter_duration_seconds"

// ScrapeLimiterHistogram reads the server-side limiter-latency histogram from
// every instance's /metrics endpoint and adds them together. source filters
// by the decision source label ("redis", "memory", ...); "" includes all.
func ScrapeLimiterHistogram(ctx context.Context, client *http.Client, metricsURLs []string, source string) (Histogram, error) {
	sum := map[float64]float64{}
	for _, u := range metricsURLs {
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		req, _ := http.NewRequestWithContext(rctx, http.MethodGet, u, nil)
		resp, err := client.Do(req)
		if err != nil {
			cancel()
			return Histogram{}, fmt.Errorf("scrape %s: %w", u, err)
		}
		p := expfmt.NewTextParser(model.UTF8Validation)
		families, err := p.TextToMetricFamilies(resp.Body)
		resp.Body.Close()
		cancel()
		if err != nil {
			return Histogram{}, fmt.Errorf("parse %s: %w", u, err)
		}
		mf, ok := families[limiterMetric]
		if !ok {
			continue
		}
		for _, m := range mf.GetMetric() {
			if source != "" && !hasLabel(m.GetLabel(), "source", source) {
				continue
			}
			for _, b := range m.GetHistogram().GetBucket() {
				if !math.IsInf(b.GetUpperBound(), 1) {
					sum[b.GetUpperBound()] += float64(b.GetCumulativeCount())
				}
			}
			// The +Inf bucket always equals the total sample count. Taking it
			// from the count (rather than from a parsed bucket that may or
			// may not be present) avoids counting it twice.
			sum[math.Inf(1)] += float64(m.GetHistogram().GetSampleCount())
		}
	}
	var h Histogram
	for b := range sum {
		h.Bounds = append(h.Bounds, b)
	}
	sort.Float64s(h.Bounds)
	for _, b := range h.Bounds {
		h.Counts = append(h.Counts, sum[b])
	}
	return h, nil
}

// Sub returns h - before, i.e. only the observations made between two scrapes.
func (h Histogram) Sub(before Histogram) Histogram {
	prev := map[float64]float64{}
	for i, b := range before.Bounds {
		prev[b] = before.Counts[i]
	}
	out := Histogram{Bounds: h.Bounds, Counts: make([]float64, len(h.Counts))}
	for i, b := range h.Bounds {
		out.Counts[i] = h.Counts[i] - prev[b]
	}
	return out
}

func (h Histogram) Total() float64 {
	if len(h.Counts) == 0 {
		return 0
	}
	return h.Counts[len(h.Counts)-1]
}

// Quantile estimates a quantile the same way PromQL's histogram_quantile
// does: find the bucket containing the rank, then interpolate linearly.
// The answer is only as precise as the bucket boundaries.
func (h Histogram) Quantile(q float64) (time.Duration, bool) {
	total := h.Total()
	if total == 0 {
		return 0, false
	}
	rank := q * total
	lower, prevCount := 0.0, 0.0
	for i, upper := range h.Bounds {
		if h.Counts[i] >= rank {
			if math.IsInf(upper, 1) {
				return time.Duration(lower * float64(time.Second)), true
			}
			inBucket := h.Counts[i] - prevCount
			frac := 1.0
			if inBucket > 0 {
				frac = (rank - prevCount) / inBucket
			}
			return time.Duration((lower + (upper-lower)*frac) * float64(time.Second)), true
		}
		lower, prevCount = upper, h.Counts[i]
	}
	return 0, false
}

func hasLabel(labels []*dto.LabelPair, name, value string) bool {
	for _, l := range labels {
		if l.GetName() == name {
			return l.GetValue() == value
		}
	}
	return false
}
