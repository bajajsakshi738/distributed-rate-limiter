.PHONY: up down logs ps test test-integration lint loadtest demo-memory demo-redis

# ---- run the whole stack locally ----
up:            ## build and start everything (dashboard http://localhost:8000)
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

# ---- tests ----
test:          ## unit tests (no Redis or Postgres needed; Redis is faked with miniredis)
	go test -race ./...

test-integration: ## multi-instance correctness + store tests against real Redis and Postgres
	docker compose -f deploy/docker-compose.test.yml up -d --wait
	REDIS_ADDR=localhost:6390 DATABASE_URL='postgres://postgres:test@localhost:5440/ratelimiter_test?sslmode=disable' \
		go test -tags integration -race -count=1 -v ./test/integration/; \
		status=$$?; docker compose -f deploy/docker-compose.test.yml down -v; exit $$status

lint:
	test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)
	go vet ./...
	go vet -tags integration ./...

# ---- command-line load test against the local stack ----
# Usage: make loadtest KEYS=ll_...,ll_... RPS=300 DURATION=20s
RPS ?= 300
DURATION ?= 20s
loadtest:
	docker compose run --rm --no-deps --entrypoint /usr/local/bin/loadgen dashboard \
		-url http://nginx:8080/api/data -keys "$(KEYS)" -rps $(RPS) -duration $(DURATION) \
		-metrics http://api-1:9090/metrics,http://api-2:9090/metrics,http://api-3:9090/metrics
