// Command loadgen runs a load test from the command line, against any
// rate limiter deployment you control. The dashboard uses the same code.
//
//	go run ./cmd/loadgen -url http://localhost:8080/api/data -keys ll_...,ll_... -rps 150 -duration 20s
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/loadgen"
)

func main() {
	url := flag.String("url", "http://localhost:8080/api/data", "endpoint to load")
	keys := flag.String("keys", os.Getenv("RATELIMITER_KEYS"), "comma-separated API keys, one per simulated client (or RATELIMITER_KEYS)")
	rps := flag.Int("rps", 100, "steady: total requests/second; boundary: requests per burst per client")
	duration := flag.Duration("duration", 10*time.Second, "test length")
	pattern := flag.String("pattern", "steady", "steady or boundary")
	metrics := flag.String("metrics", "", "optional comma-separated /metrics URLs to report server-side limiter p99")
	storeFilter := flag.String("source", "redis", "which decisions' latency to report from -metrics: redis, memory, fallback, or empty for all")
	flag.Parse()

	var ks []string
	for _, k := range strings.Split(*keys, ",") {
		if k = strings.TrimSpace(k); k != "" {
			ks = append(ks, k)
		}
	}
	if len(ks) == 0 {
		fmt.Fprintln(os.Stderr, "no keys given: use -keys or RATELIMITER_KEYS")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 2000, MaxIdleConns: 2000}}

	var metricURLs []string
	for _, m := range strings.Split(*metrics, ",") {
		if m = strings.TrimSpace(m); m != "" {
			metricURLs = append(metricURLs, m)
		}
	}
	var before loadgen.Histogram
	if len(metricURLs) > 0 {
		var err error
		if before, err = loadgen.ScrapeLimiterHistogram(ctx, client, metricURLs, *storeFilter); err != nil {
			fmt.Fprintln(os.Stderr, "metrics:", err)
		}
	}

	res, err := loadgen.Run(ctx, loadgen.Config{Target: *url, Keys: ks, RPS: *rps, Duration: *duration, Pattern: *pattern}, client)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("sent %d in %v (%.0f req/s)\n", res.Sent, res.Elapsed.Round(time.Millisecond), float64(res.Sent)/res.Elapsed.Seconds())
	fmt.Printf("allowed %d (%.1f/s)  rejected %d  errors %d\n", res.Allowed, float64(res.Allowed)/duration.Seconds(), res.Rejected, res.Errors)
	fmt.Printf("client latency p50 %v  p99 %v\n", res.P50, res.P99)
	fmt.Printf("peak allowed for one client in any 1s span: %d\n", res.Peak1sAllowed)

	perInstance := map[string][3]int{}
	for _, b := range res.Buckets {
		v := perInstance[b.Instance]
		v[0] += b.Allowed
		v[1] += b.Rejected
		v[2] += b.Errors
		perInstance[b.Instance] = v
	}
	for inst, v := range perInstance {
		fmt.Printf("  %-12s allowed %6d  rejected %6d  errors %d\n", inst, v[0], v[1], v[2])
	}

	if len(metricURLs) > 0 {
		after, err := loadgen.ScrapeLimiterHistogram(ctx, client, metricURLs, *storeFilter)
		if err != nil {
			fmt.Fprintln(os.Stderr, "metrics:", err)
			return
		}
		d := after.Sub(before)
		p50, _ := d.Quantile(0.5)
		p99, _ := d.Quantile(0.99)
		fmt.Printf("server-side limiter call: p50 %v  p99 %v  (%.0f observations, estimated from histogram buckets)\n", p50, p99, d.Total())
	}
}
