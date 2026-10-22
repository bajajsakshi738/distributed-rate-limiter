# Security

This rate limiter is a portfolio project that runs publicly. This file lists what protects it and what it deliberately doesn't do.

## Measures

**Load generator**
- The target is fixed by configuration (`LOADTEST_TARGET`, the internal nginx). There is no URL field, so it can't be
  pointed at other sites.
- Admin only. Caps: 1,000 req/s (configurable), 60 seconds, 50 clients, one run at a time.
- Each run creates temporary keys and revokes them when it ends. On startup, leftover keys from a crashed run are revoked.

**Dashboard**
- **The admin pages are not reachable from the internet.** Caddy answers `/admin` with 404; the owner reaches them
  through an SSH tunnel to the dashboard port, which is bound to `127.0.0.1` on the server (see docs/DEPLOY.md). So
  nobody can brute-force the login or keep it locked from outside.
- Admin pages use HTTP basic auth. Credentials come from environment variables and are compared in
  constant time (both sides are hashed first, so length differences don't leak through timing).
- Failed logins are limited globally (5 attempts, refilling one every 12s), using this project's own token bucket.
  When exhausted, the admin pages lock for at least 30 seconds, even for the right password.
- Every admin route goes through one auth middleware; a test checks that every route rejects unauthenticated requests.
- CSRF: every POST needs an HMAC token tied to a server secret. Requests whose `Origin` or `Sec-Fetch-Site` says they
  came from another site are refused.
- `html/template` escapes all output. The SVG charts are built from numbers and escaped text; a test checks that a
  malicious marker label is escaped.
- A newly created API key is rendered directly in the response, never placed in a URL, and shown only once.
- The public live view is cached for one second and shared by all viewers, so many viewers can't multiply the load.
- Strict Content Security Policy (`script-src 'self'; style-src 'self'`, no inline styles or scripts,
  `frame-ancestors 'none'`). htmx is served from the dashboard itself, configured not to inject styles or use `eval`.

**API keys**
- Generated with `crypto/rand` (30 base62 characters, ~178 bits). Only the SHA-256 hash is stored. A fast hash is
  fine because the keys are random and long; bcrypt is for low-entropy human passwords.
- Keys are accepted only in the `Authorization` header. Requests with a key in the query string get 400, because URLs
  end up in logs and browser history.
- Only the 11-character prefix is ever logged or displayed.
- Keys carry a 6-character HMAC-SHA256 signature made with a server secret (`KEY_SECRET`). Only server-issued keys pass
  the format check, so a flood of made-up keys is rejected in microseconds and never reaches the caches or Postgres.
  Rotating `KEY_SECRET` invalidates every key, so it is generated once by `scripts/init-env.sh`.
- Concurrent cache misses for the same key share one database query (singleflight), and rule edits don't clear the key
  caches, so a config change can't cause a burst of database lookups.
- Revocation applies within about 2 seconds, or within 60 seconds if Redis is down.
- A demo key (3 requests per 10 seconds) powers the "Try it yourself" button. It never leaves the server: the
  dashboard sends the request on the visitor's behalf, at most 20 at a time. It's derived from `KEY_SECRET`, can be
  revoked from the admin page, and can be turned off with `DEMO_KEY=off`.

**Data stores**
- All SQL is parameterized (pgx). Rule values are validated in the handler and by `CHECK` constraints.
- Postgres and Redis are on a Docker network with `internal: true` (no internet access) and no published ports.
  Both require passwords.

**HTTP servers**
- Read-header, read, write and idle timeouts on every Go server (slowloris protection), and header size limits.
- Request bodies are capped (1 KB on the API, 16 KB on the dashboard).
- Clients get generic error messages; details go to server logs.
- `/metrics` and `/internal/stats` are served on a separate internal port (9090) that nginx never proxies.

**Deployment**
- HTTPS by Caddy with automatic Let's Encrypt certificates, plus HSTS.
- Secrets live in `.env` (mode 600, git-ignored), generated randomly by `scripts/init-env.sh`.
- Go services run as non-root on distroless images, with read-only filesystems, all Linux capabilities dropped,
  `no-new-privileges`, and memory and CPU limits. nginx uses the unprivileged image. Logs rotate.
- Only ports 80, 443 and SSH are open on the server (see docs/DEPLOY.md). SSH is key-only.
- `govulncheck` runs in CI.

## Known limitations

- **No IP-based limiting.** Limits are per API key. Avoiding IP limiting avoids trusting `X-Forwarded-For` headers, but
  it means requests without a valid key, and the public read-only pages, aren't rate-limited per client. Both are cheap
  to serve (a signature check; a page cached for 1 second), so the main cost of a flood is bandwidth and CPU on a small
  VM, not the database. Adding `limit_req` per IP in nginx (with `set_real_ip_from` the Caddy network) is the next step
  if that becomes a problem.
- **Postgres role.** The apps connect as the database owner created by the Postgres image, not a separate
  least-privilege role. Postgres is only reachable on the internal Docker network.
- **Redis is a single point of failure.** Mitigated by the circuit breaker and fallback mode, not by replication.
- **Basic auth** has no logout or session expiry. Acceptable for a single admin behind an SSH tunnel; a real product
  would use sessions.
- **Fallback mode** headers describe the local instance's share of the limit, not the global limit.

## Reporting

If you find a security issue, please open a GitHub issue without exploit details, or contact me directly.
