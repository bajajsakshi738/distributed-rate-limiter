# Distributed Rate Limiter

A multi-instance API rate limiter in Go. It runs as three instances behind a load balancer, shows the classic bug
where in-memory limits break across instances, fixes it with shared state in Redis, and proves the fix with tests,
load tests and a live dashboard.

**Live demo:** https://YOUR-DOMAIN.duckdns.org (click "Send a request" a few times and watch your own requests get 429s) &nbsp;·&nbsp; **Stack:** Go 1.26, Redis (Lua), Postgres, nginx, Caddy, Docker Compose,
htmx, Prometheus

<!-- Replace with a 20-30s GIF of your showcase replay (docs/demo.gif) once deployed. -->
![A recorded load test: memory mode lets ~3x the limit through, then switching to Redis brings it back to the limit](docs/screenshot-run.png)

## The story in 30 seconds

Every public API needs a rule like "each customer may make 10 requests per second; anything extra gets HTTP 429".

1. **The bug.** I run the API as 3 instances behind round-robin nginx. If each instance counts requests in its own
   memory, a client limited to 10 req/s gets about **30 req/s** through, because each instance lets 10 through.
2. **The fix.** All instances share one counter in Redis, updated by an atomic Lua script. The limit holds at 10 req/s
   no matter which instance a request lands on.
3. **The proof.** An automated test fires traffic through a load balancer at 3 instances and asserts that allowed
   requests never exceed `burst + rate × time` with Redis, and are about 3× that in memory mode. The dashboard records
   load tests and replays them.

## Results

Numbers from my own load tests (Oracle Cloud Always Free ARM VM, 2 OCPUs, everything on one machine, so treat them as
relative rather than absolute):

| Measurement | Result | How it was measured |
|---|---|---|
| Memory mode, 5 clients × 10 req/s limit, 300 req/s offered | **`<fill in>` req/s allowed (≈3×)** | dashboard load test, per-second counts |
| Redis mode, same load | **`<fill in>` req/s allowed** | same run, after switching store |
| Redis limiter call overhead, p99 | **`<fill in>` ms** | server-side Prometheus histogram around the limiter call only, Redis-decided requests |
| Memory limiter call, p99 (same box, for comparison) | `<fill in>` µs | same histogram, memory mode — the Redis figure is best read *relative* to this |
| Client-side latency through nginx, p50 / p99 | `<fill in>` / `<fill in>` ms | open-loop load generator, measured from scheduled send time |
| Fixed window, peak allowed in any 1s span (limit 10) | **`<fill in>`** (≈2×) | boundary-burst load pattern |
| Token bucket, same pattern | `<fill in>` | same |
| Key cache hit ratio during a load run | `<fill in>`% | `ratelimiter_key_lookups_total` counters |
| Redis killed mid-test (fallback mode) | `<fill in>` req/s allowed, 0 errors | dashboard load test + `docker compose stop redis` |

## Architecture

```
Internet ──► Caddy (HTTPS, Let's Encrypt)
               ├── /api/*  ──► nginx (round-robin) ──► api-1 ┐
               │                                   ├──► api-2 ├──► Redis     shared limiter state (Lua)
               │                                   └──► api-3 ┘    Postgres  rules, plans, keys, runs
               └── /*      ──► dashboard ── polls api-1/2/3 :9090/internal/stats every second
                                         └── runs load tests against nginx (fixed target)
```

8 containers: `api ×3`, `nginx`, `dashboard`, `postgres`, `redis`, `caddy`. Only Caddy is reachable from the
internet. Postgres and Redis sit on a Docker network with no internet access and no published ports.

### What happens on one request

1. nginx sends `GET /api/data` to one of the 3 instances, round-robin.
2. The instance checks the key's format and signature (`ll_<30 random chars><6 char HMAC>`). The signature is an
   HMAC with a server secret, so made-up keys, typos and junk are rejected here, without touching a cache or the database.
3. It looks the key up by SHA-256 hash: in-memory cache first (60s TTL, LRU-bounded), Postgres on a miss. Concurrent
   misses for the same key share one query (singleflight). "Not found" results are cached too, in a separate size-limited cache.
4. It finds the key's plan and rule, and asks the limiter: allowed?
5. It answers `200` or `429`, with `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset` and, on 429, `Retry-After`.
   (In fail-closed mode with Redis down it answers `503`: the client didn't exceed anything.)
6. It updates in-memory per-second stats and Prometheus metrics. No extra Redis or database calls.

## Design decisions

**Atomic Lua scripts.** Reading a bucket, refilling it and writing it back must be one step. With separate `GET` and
`SET` calls, two instances can both read "1 token left" and both allow a request. A Lua script runs atomically inside
Redis, and it's one network round trip.

**Redis's clock, not the instances' clocks.** The scripts call `redis.call('TIME')`. If each instance passed its own
time, instances with drifting clocks would disagree about refill amounts and window boundaries. Redis 5+ replicates
script effects rather than the script itself, so this is safe with replicas.

**Two algorithms.** Token bucket (smooth, allows a configurable burst) and fixed window (simplest). The fixed window's
known flaw is visible in the boundary-burst demo: a client can get 2× the limit through within ~100ms by bursting at
the end of one window and the start of the next. The token bucket's worst case is `burst + rate × time`, which the
operator controls with `burst`.

**No unbounded growth.** Every Redis key has a TTL. A token bucket expires once it would be full again (a full bucket
is the same as a missing one). The fixed-window counter stores its window number inside the hash, so a script touches
only the one key it declares; the window length is part of the key, so editing a rule's window starts fresh counters.
The in-memory store is swept every 30 seconds.

**Why a token bucket and not GCRA?** GCRA (what `go-redis/redis_rate` uses) is the compact equivalent: one integer
timestamp instead of a float token count. I chose the token bucket because its state is easier to explain and show on a
chart; the behaviour is the same.

**When Redis fails.** Each Redis call has a 15ms timeout, behind a circuit breaker that opens when, within one second,
at least 10 calls failed *and* they were at least half of all calls; it retries with one probe every 2 seconds. A ratio
rather than "N failures in a row" matters at high traffic: a 15ms pause at 1,000 req/s fails ~15 calls at once, which
shouldn't open the breaker. Note that a timeout doesn't mean the script didn't run: Redis may still have consumed a
token after the client gave up and fell back. Then a global setting decides:

| Mode | Behaviour | Trade-off |
|---|---|---|
| `fallback` (default) | each instance limits locally at `limit ÷ INSTANCE_COUNT` | approximate but bounded |
| `open` | allow everything | availability over protection |
| `closed` | reject everything with `503` | protection over availability |

**Stats stay out of Redis.** Live per-second counts and recent requests are kept in each instance's memory, and the
dashboard polls the instances. So when Redis is killed for the failure demo, the dashboard keeps working, which is
exactly when it matters.

**Config changes.** Rules and keys are edited in the dashboard and saved to Postgres, then a new random value is written
to `config:version` (rules, settings) or `keys:version` (revocations) in Redis. Random values, not a counter: a counter
restarts at 1 when a non-persistent Redis restarts, and an instance that last saw 1 would miss the change. Each
instance checks both every 2 seconds, reading the versions *before* loading so a change committed mid-load isn't
marked as seen. A config change reloads rules; only a keys change clears the key caches, so a rule edit doesn't send
every in-flight key to Postgres at once. A full reload every 60 seconds is the backup when Redis is down. If Postgres
is down, instances keep the last config they loaded.

**Aggregates, not request logs.** Load-test results are stored per second, per instance (about 180 rows for a 60s
run) instead of one row per request (60,000 rows at 1,000 req/s).

**Open-loop load generator.** Requests are sent on a fixed schedule and latency is measured from the *scheduled* time.
A closed-loop generator (send, wait, send) slows down when the server does and under-reports latency, a problem
known as coordinated omission.

## Run it locally

You need Docker with Compose v2.

```bash
./scripts/init-env.sh          # creates .env with random passwords and prints the admin login
make up                        # or: docker compose up -d --build
```

- Dashboard: http://localhost:8000 (admin pages at http://localhost:8000/admin)
- API: `curl -H "Authorization: Bearer <key>" http://localhost:8080/api/data`

The live page shows the chart and four numbers: allowed, rejected, **got through** (allowed ÷ the most the limit
permits, so about 1.00× with Redis and about 3× with in-memory counting) and the key cache hit rate. During a load test
they're live; otherwise the page replays a saved test, and every number comes from that test, nothing live.

It also has a "Try it yourself" button: each click sends one real request with a demo key that allows 3 requests per 10 seconds, so a few quick clicks show the 429s.

## The three demos

All from the admin page.

1. **The 3× bug and the fix.** Set *Store* to `memory`. Start a load test: plan `free`, 5 clients, steady, 300 req/s,
   30s. About 150 req/s get through (3 × 5 clients × 10), and *Got through* reads about 3×. Halfway through, set *Store* to `redis`. Allowed drops to
   about 50 req/s. The switch is recorded as a marker on the run. Pin the run as the showcase, and visitors see it
   replayed whenever there's no live traffic.
2. **Fixed-window boundary burst.** Set the `free` rule to `fixed window`. Run the `boundary` pattern (2 clients, burst
   size 30, 10s). *Peak in any 1s, one client* is ~20, twice the limit. Switch back to token bucket and repeat: ~10.
3. **Redis outage.** Start a steady load test, then run `docker compose stop redis`. A warning appears on the live page,
   the breaker opens, allowed traffic drops slightly to the fallback level, and there are no errors. The moment is recorded
   as a marker. `docker compose start redis` and the breaker closes again.

## Tests

```bash
make test               # unit tests, no services needed (Redis faked with miniredis, which runs the real Lua)
make test-integration   # real Redis + Postgres in Docker
```

The integration suite includes the **multi-instance correctness test**
([test/integration/correctness_test.go](test/integration/correctness_test.go)): three API instances, each with its
own limiter and Redis connection pool, behind a round-robin reverse proxy, loaded at 10× the limit with concurrent
requests. With Redis, allowed ≤ `burst + rate × elapsed + 1` in total *and* in every sliding 1-second span; in memory
mode, ≥ 2.5× the bound. It also asserts that every allowed response was decided by the store under test (so fallback
can't pass for Redis) and that each instance served about a third of the traffic (so a broken proxy can't make any
limiter look correct). It also checks the boundary burst against real Redis, and that fallback mode limits each
instance to its share when Redis is unreachable.

Other tests cover: both algorithms in both stores (including that memory and Redis give identical decisions step by
step), token clamping when a limit is lowered, the circuit breaker (including that a short blip doesn't open it), forged
keys never reaching the database, singleflight lookups, config-reload ordering, revocation, headers, the admin pages'
auth, CSRF and lockout, chart escaping, the replay clock and the load generator.

CI (GitHub Actions) runs `gofmt`, `go vet`, unit and integration tests with the race detector, `govulncheck`, a compose
config check and a Docker build.

## Project layout

```
cmd/api            API instance (public :8080, internal :9090)
cmd/dashboard      dashboard + load-test runner
cmd/loadgen        command-line load generator
internal/limiter   token bucket + fixed window, memory and Redis stores, Lua scripts, breaker, failure modes
internal/api       /api/data, the rate-limit middleware, /internal/stats, /metrics
internal/apikey    key generation, HMAC signature, hashing
internal/catalog   current rules/plans/settings and the reload loop
internal/keycache  bounded LRU with TTL
internal/stats     per-instance per-second counters
internal/store     Postgres queries and migrations
internal/loadgen   open-loop load generator, histogram scraping
internal/dashboard pages, SVG charts, replay, admin, auth
deploy/            nginx.conf, Caddyfile, test compose file
test/integration   tests against real Redis and Postgres
```

## Limitations

- **Redis is a single point of failure.** Mitigated by the breaker and fallback mode, not by replication.
- **One machine.** All 3 instances run on one VM. The coordination problem (independent processes sharing mutable
  state) is the same as across machines, but this setup doesn't exercise real network latency, partitions, or Redis on
  another host, and the latency numbers are best read as relative.
- **No IP-based limiting.** Limits are per API key only. Requests without a validly signed key are rejected cheaply in
  Go, but they aren't rate-limited by IP (see SECURITY.md).
- **Fallback is approximate.** It assumes traffic is spread evenly and `INSTANCE_COUNT` is correct. Rate and burst are
  both divided; a limit smaller than the instance count over-allows (each share is at least 1); if an instance dies,
  the others still use 1/3 each.
- **Transitions allow one extra burst.** When switching to fallback or back, the new buckets start full.
- **Run markers are approximate** (±2s), because instances pick up a store switch on their next poll.

## Why not just use redis_rate, Envoy or nginx limit_req?

In production I probably would. I built this to understand the atomicity and failure semantics myself; the
memory-versus-Redis equivalence test and the multi-instance test are how I checked my implementation.

## What I'd do next

- Sliding-window-counter algorithm as a third option.
- Redis replication with Sentinel, and measuring failover behaviour.
- Local pre-limiting with periodic sync to Redis, to cut the per-request round trip at very high rates.
- Grafana dashboards on the existing Prometheus metrics.
