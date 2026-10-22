-- Rules: what a limit looks like. A plan points to one rule.
CREATE TABLE rules (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    algorithm   TEXT NOT NULL CHECK (algorithm IN ('token_bucket', 'fixed_window')),
    limit_count INT  NOT NULL CHECK (limit_count BETWEEN 1 AND 1000000),
    window_ms   INT  NOT NULL CHECK (window_ms BETWEEN 100 AND 86400000),
    burst       INT  NOT NULL DEFAULT 0 CHECK (burst BETWEEN 0 AND 1000000),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE plans (
    id      BIGSERIAL PRIMARY KEY,
    name    TEXT NOT NULL UNIQUE,
    rule_id BIGINT NOT NULL REFERENCES rules(id)
);

-- Global settings edited from the dashboard (store mode, Redis failure mode).
CREATE TABLE settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- One row per load test. Totals are filled in when the run finishes.
CREATE TABLE load_runs (
    id              BIGSERIAL PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'done', 'failed')),
    plan_name       TEXT NOT NULL,
    clients         INT  NOT NULL,
    rps             INT  NOT NULL,
    duration_s      INT  NOT NULL,
    pattern         TEXT NOT NULL,
    store           TEXT NOT NULL,
    algorithm       TEXT NOT NULL,
    limit_desc      TEXT NOT NULL DEFAULT '',
    sent            BIGINT NOT NULL DEFAULT 0,
    allowed         BIGINT NOT NULL DEFAULT 0,
    rejected        BIGINT NOT NULL DEFAULT 0,
    errors          BIGINT NOT NULL DEFAULT 0,
    p50_us          INT,
    p99_us          INT,
    limiter_p99_us  INT,
    peak_1s_allowed INT,
    is_showcase     BOOLEAN NOT NULL DEFAULT false,
    note            TEXT NOT NULL DEFAULT '',
    marker_second   INT,
    marker_label    TEXT NOT NULL DEFAULT ''
);

-- At most one pinned showcase run.
CREATE UNIQUE INDEX load_runs_one_showcase ON load_runs ((true)) WHERE is_showcase;

-- Per-second, per-instance results of a run. Aggregates, not one row per
-- request, so a 60s run at 2,000 req/s is ~180 rows instead of 120,000.
CREATE TABLE run_seconds (
    run_id   BIGINT NOT NULL REFERENCES load_runs(id) ON DELETE CASCADE,
    second   INT    NOT NULL,
    instance TEXT   NOT NULL,
    allowed  INT    NOT NULL,
    rejected INT    NOT NULL,
    errors   INT    NOT NULL,
    PRIMARY KEY (run_id, second, instance)
);

-- Only the SHA-256 hash of a key is stored. prefix is the safe-to-show part.
CREATE TABLE api_keys (
    id         BIGSERIAL PRIMARY KEY,
    key_hash   BYTEA  NOT NULL UNIQUE,
    prefix     TEXT   NOT NULL,
    owner      TEXT   NOT NULL,
    plan_id    BIGINT NOT NULL REFERENCES plans(id),
    run_id     BIGINT REFERENCES load_runs(id) ON DELETE SET NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX api_keys_run_id ON api_keys (run_id) WHERE run_id IS NOT NULL;

INSERT INTO rules (name, algorithm, limit_count, window_ms, burst) VALUES
    ('free', 'token_bucket', 10, 1000, 10),
    ('pro',  'token_bucket', 100, 1000, 100);

INSERT INTO plans (name, rule_id)
    SELECT name, id FROM rules WHERE name IN ('free', 'pro');

INSERT INTO settings (key, value) VALUES ('failure_mode', 'fallback');
