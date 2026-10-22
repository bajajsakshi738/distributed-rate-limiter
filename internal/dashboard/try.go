package dashboard

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"time"
)

// tryResult is one line in the "Try it yourself" box.
type tryResult struct {
	Code     int
	Allowed  bool
	Instance string
	Left     string // RateLimit-Remaining
	RetryIn  string // Retry-After, seconds
	Problem  string // set when the request itself failed
}

// tryClient sends the visitor's request to the API on their behalf.
var tryClient = &http.Client{Timeout: 2 * time.Second}

// handleTry sends ONE real request through nginx to the API, using the public
// demo key, and returns a single result line for htmx to add to the page.
//
// The visitor's browser never sees the key and nothing runs on their
// computer. Abuse is bounded: the demo key itself allows 5 req/s, and at most
// tryMaxInFlight of these requests are in flight at once across all visitors.
func (s *Server) handleTry(w http.ResponseWriter, r *http.Request) {
	if s.opt.DemoKey == "" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")

	var res tryResult
	select {
	case s.trySlots <- struct{}{}:
		res = s.sendTry(r.Context())
		<-s.trySlots
	default:
		res = tryResult{Problem: "busy, try again in a moment"}
	}
	s.render(w, "try_result.html", res)
}

const tryMaxInFlight = 20

func (s *Server) sendTry(ctx context.Context) tryResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.opt.LoadTarget, nil)
	if err != nil {
		return tryResult{Problem: "could not build the request"}
	}
	req.Header.Set("Authorization", "Bearer "+s.opt.DemoKey)
	resp, err := tryClient.Do(req)
	if err != nil {
		return tryResult{Problem: "the API didn't answer"}
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	resp.Body.Close()

	res := tryResult{
		Code:     resp.StatusCode,
		Allowed:  resp.StatusCode == http.StatusOK,
		Instance: resp.Header.Get("X-Instance"),
		Left:     resp.Header.Get("RateLimit-Remaining"),
		RetryIn:  resp.Header.Get("Retry-After"),
	}
	if res.Instance == "" {
		res.Instance = "unknown"
	}
	if _, err := strconv.Atoi(res.RetryIn); err != nil {
		res.RetryIn = "1"
	}
	return res
}
