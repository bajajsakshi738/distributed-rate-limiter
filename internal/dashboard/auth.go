package dashboard

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
)

// adminAuth protects /admin with HTTP basic auth, a global limit on failed
// logins, and CSRF tokens on every POST.
//
// The failed-login limit is global, not per IP (this project deliberately
// does no IP-based limiting). The trade-off: an attacker can lock the admin
// pages for a few minutes, but can't brute-force the password. The owner can
// still reach the server over SSH.
type adminAuth struct {
	userHash, passHash [32]byte
	csrfKey            []byte

	failures    *limiter.Memory
	failureRule limiter.Rule

	mu          sync.Mutex
	lockedUntil time.Time
	now         func() time.Time
}

func newAdminAuth(user, pass string, csrfKey []byte) *adminAuth {
	return &adminAuth{
		userHash: sha256.Sum256([]byte(user)),
		passHash: sha256.Sum256([]byte(pass)),
		csrfKey:  csrfKey,
		failures: limiter.NewMemory(),
		// 5 failed attempts, refilling one every 12 seconds.
		failureRule: limiter.Rule{ID: 1, Algorithm: limiter.TokenBucket, Limit: 5, Window: time.Minute, Burst: 5},
		now:         time.Now,
	}
}

func (a *adminAuth) check(r *http.Request) bool {
	u, p, ok := r.BasicAuth()
	if !ok {
		return false
	}
	// Hash both sides first so the comparison is constant-time even when the
	// lengths differ, then compare without short-circuiting.
	uh, ph := sha256.Sum256([]byte(u)), sha256.Sum256([]byte(p))
	userOK := subtle.ConstantTimeCompare(uh[:], a.userHash[:])
	passOK := subtle.ConstantTimeCompare(ph[:], a.passHash[:])
	return userOK&passOK == 1
}

func (a *adminAuth) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")

		a.mu.Lock()
		locked := a.now().Before(a.lockedUntil)
		wait := a.lockedUntil.Sub(a.now())
		a.mu.Unlock()
		if locked {
			w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
			http.Error(w, "too many failed logins, try again later", http.StatusTooManyRequests)
			return
		}

		if !a.check(r) {
			if _, _, attempted := r.BasicAuth(); attempted {
				d, _ := a.failures.Allow(context.Background(), a.failureRule, "admin")
				if !d.Allowed {
					a.mu.Lock()
					a.lockedUntil = a.now().Add(max(d.RetryAfter, 30*time.Second))
					a.mu.Unlock()
				}
			}
			w.Header().Set("WWW-Authenticate", `Basic realm="Rate limiter admin", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		if r.Method == http.MethodPost && !a.validCSRF(r) {
			http.Error(w, "invalid or missing CSRF token", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// csrfToken is an HMAC of the admin user name with a server secret. A page
// on another site can make the browser send a POST with the admin's basic
// auth credentials, but it can't read our pages to learn this token.
func (a *adminAuth) csrfToken() string {
	m := hmac.New(sha256.New, a.csrfKey)
	m.Write([]byte("csrf"))
	m.Write(a.userHash[:])
	return hex.EncodeToString(m.Sum(nil))
}

func (a *adminAuth) validCSRF(r *http.Request) bool {
	// Second layer: modern browsers say where a request came from.
	if site := r.Header.Get("Sec-Fetch-Site"); site != "" && site != "same-origin" && site != "none" {
		return false
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil || u.Host != r.Host {
			return false
		}
	}
	got := r.PostFormValue("csrf_token")
	return hmac.Equal([]byte(got), []byte(a.csrfToken()))
}
