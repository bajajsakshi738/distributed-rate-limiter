// Package store is the Postgres data layer. Every query is parameterized;
// no SQL is ever built from user input.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bajajsakshi738/distributed-rate-limiter/internal/apikey"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/catalog"
	"github.com/bajajsakshi738/distributed-rate-limiter/internal/limiter"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

var ErrNotFound = errors.New("not found")

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, url string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	if maxConns > 0 {
		// Bounds how hard a flood of uncached key lookups can hit Postgres.
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate applies pending migrations. An advisory lock stops several
// instances starting at the same time from racing each other.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(727272)`); err != nil {
		return err
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(727272)`)

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		version, err := strconv.Atoi(strings.SplitN(name, "_", 2)[0])
		if err != nil {
			return fmt.Errorf("bad migration name %q", name)
		}
		var exists bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sql, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
			tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// ---- configuration ----

func (s *Store) LoadSnapshot(ctx context.Context) (*catalog.Snapshot, error) {
	snap := &catalog.Snapshot{
		Rules:    map[int64]limiter.Rule{},
		Plans:    map[int64]catalog.Plan{},
		Settings: catalog.Settings{FailureMode: limiter.FailFallback},
		LoadedAt: time.Now(),
	}
	rules, err := s.ListRules(ctx)
	if err != nil {
		return nil, err
	}
	for _, r := range rules {
		snap.Rules[r.ID] = r
	}
	plans, err := s.ListPlans(ctx)
	if err != nil {
		return nil, err
	}
	for _, p := range plans {
		snap.Plans[p.ID] = p
	}
	settings, err := s.Settings(ctx)
	if err != nil {
		return nil, err
	}
	snap.Settings = settings
	return snap, nil
}

func (s *Store) ListRules(ctx context.Context) ([]limiter.Rule, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name, algorithm, limit_count, window_ms, burst FROM rules ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (limiter.Rule, error) {
		var r limiter.Rule
		var algo string
		var windowMs int
		err := row.Scan(&r.ID, &r.Name, &algo, &r.Limit, &windowMs, &r.Burst)
		r.Algorithm = limiter.Algorithm(algo)
		r.Window = time.Duration(windowMs) * time.Millisecond
		return r, err
	})
}

func (s *Store) UpdateRule(ctx context.Context, r limiter.Rule) error {
	if err := r.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE rules SET algorithm=$2, limit_count=$3, window_ms=$4, burst=$5, updated_at=now() WHERE id=$1`,
		r.ID, string(r.Algorithm), r.Limit, r.Window.Milliseconds(), r.Burst)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ListPlans(ctx context.Context) ([]catalog.Plan, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, name, rule_id FROM plans ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (catalog.Plan, error) {
		var p catalog.Plan
		err := row.Scan(&p.ID, &p.Name, &p.RuleID)
		return p, err
	})
}

func (s *Store) PlanByName(ctx context.Context, name string) (catalog.Plan, error) {
	var p catalog.Plan
	err := s.pool.QueryRow(ctx, `SELECT id, name, rule_id FROM plans WHERE name=$1`, name).Scan(&p.ID, &p.Name, &p.RuleID)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, ErrNotFound
	}
	return p, err
}

func (s *Store) Settings(ctx context.Context) (catalog.Settings, error) {
	st := catalog.Settings{FailureMode: limiter.FailFallback}
	rows, err := s.pool.Query(ctx, `SELECT key, value FROM settings`)
	if err != nil {
		return st, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return st, err
		}
		switch k {
		case "store":
			st.Store = v
		case "failure_mode":
			if m := limiter.FailureMode(v); m.Valid() {
				st.FailureMode = m
			}
		}
	}
	return st, rows.Err()
}

func (s *Store) SetSetting(ctx context.Context, key, value string) error {
	switch key {
	case "store":
		if value != "memory" && value != "redis" {
			return errors.New("store must be memory or redis")
		}
	case "failure_mode":
		if !limiter.FailureMode(value).Valid() {
			return errors.New("failure mode must be fallback, open or closed")
		}
	default:
		return errors.New("unknown setting")
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO settings (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, key, value)
	return err
}

// ---- API keys ----

type KeyRecord struct {
	ID     int64
	PlanID int64
	Prefix string
}

// LookupKey finds an active (not revoked) key by its hash.
func (s *Store) LookupKey(ctx context.Context, hash []byte) (KeyRecord, bool, error) {
	var k KeyRecord
	err := s.pool.QueryRow(ctx, `SELECT id, plan_id, prefix FROM api_keys WHERE key_hash=$1 AND revoked_at IS NULL`, hash).
		Scan(&k.ID, &k.PlanID, &k.Prefix)
	if errors.Is(err, pgx.ErrNoRows) {
		return k, false, nil
	}
	return k, err == nil, err
}

type KeyInfo struct {
	ID        int64
	Prefix    string
	Owner     string
	Plan      string
	RunID     *int64
	Revoked   bool
	CreatedAt time.Time
}

// CreateKey stores a new key (generated by the caller with its apikey.Signer).
// Only the hash is stored; the raw value can't be shown again.
func (s *Store) CreateKey(ctx context.Context, raw, owner string, planID int64, runID *int64) (KeyInfo, error) {
	info := KeyInfo{Prefix: apikey.DisplayPrefix(raw), Owner: owner, RunID: runID}
	err := s.pool.QueryRow(ctx, `INSERT INTO api_keys (key_hash, prefix, owner, plan_id, run_id) VALUES ($1,$2,$3,$4,$5)
		RETURNING id, created_at, (SELECT name FROM plans WHERE id=$4)`,
		apikey.Hash(raw), info.Prefix, owner, planID, runID).Scan(&info.ID, &info.CreatedAt, &info.Plan)
	return info, err
}

// EnsureKey inserts a key if it doesn't exist yet (used for the public demo
// key). If an admin revoked it, it stays revoked.
func (s *Store) EnsureKey(ctx context.Context, raw, owner string, planID int64) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO api_keys (key_hash, prefix, owner, plan_id) VALUES ($1,$2,$3,$4)
		ON CONFLICT (key_hash) DO NOTHING`, apikey.Hash(raw), apikey.DisplayPrefix(raw), owner, planID)
	return err
}

// CreateRunKeys stores the temporary keys for a load test in one round trip.
func (s *Store) CreateRunKeys(ctx context.Context, keys []string, planID, runID int64) error {
	rows := make([][]any, len(keys))
	for i, raw := range keys {
		rows[i] = []any{apikey.Hash(raw), apikey.DisplayPrefix(raw), "loadtest-" + strconv.FormatInt(runID, 10), planID, runID}
	}
	_, err := s.pool.CopyFrom(ctx, pgx.Identifier{"api_keys"},
		[]string{"key_hash", "prefix", "owner", "plan_id", "run_id"}, pgx.CopyFromRows(rows))
	return err
}

func (s *Store) ListKeys(ctx context.Context, limit int) ([]KeyInfo, error) {
	rows, err := s.pool.Query(ctx, `SELECT k.id, k.prefix, k.owner, p.name, k.run_id, k.revoked_at IS NOT NULL, k.created_at
		FROM api_keys k JOIN plans p ON p.id = k.plan_id
		WHERE k.run_id IS NULL
		ORDER BY k.id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (KeyInfo, error) {
		var k KeyInfo
		err := row.Scan(&k.ID, &k.Prefix, &k.Owner, &k.Plan, &k.RunID, &k.Revoked, &k.CreatedAt)
		return k, err
	})
}

func (s *Store) RevokeKey(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx, `UPDATE api_keys SET revoked_at=now() WHERE id=$1 AND revoked_at IS NULL`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) RevokeRunKeys(ctx context.Context, runID int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE api_keys SET revoked_at=now() WHERE run_id=$1 AND revoked_at IS NULL`, runID)
	return err
}

// CleanupOrphans runs at dashboard startup: a run that was still "running"
// when the process died is marked failed, and its temporary keys revoked.
func (s *Store) CleanupOrphans(ctx context.Context) (int64, error) {
	if _, err := s.pool.Exec(ctx, `UPDATE load_runs SET status='failed', finished_at=now() WHERE status='running'`); err != nil {
		return 0, err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE api_keys SET revoked_at=now()
		WHERE run_id IS NOT NULL AND revoked_at IS NULL`)
	return tag.RowsAffected(), err
}

// ---- load runs ----

type Run struct {
	ID            int64
	StartedAt     time.Time
	FinishedAt    *time.Time
	Status        string
	PlanName      string
	Clients       int
	RPS           int
	DurationS     int
	Pattern       string
	Store         string
	Algorithm     string
	LimitDesc     string
	Sent          int64
	Allowed       int64
	Rejected      int64
	Errors        int64
	P50us         *int
	P99us         *int
	LimiterP99us  *int
	Peak1sAllowed *int
	IsShowcase    bool
	Note          string
	MarkerSecond  *int
	MarkerLabel   string
	LimitRatio    *float64 // allowed ÷ the most the limit permits; nil for older runs
	CacheHitPct   *float64 // % of key lookups served from the cache during the run
}

type RunSecond struct {
	Second   int
	Instance string
	Allowed  int
	Rejected int
	Errors   int
}

const runColumns = `id, started_at, finished_at, status, plan_name, clients, rps, duration_s, pattern, store, algorithm, limit_desc,
	sent, allowed, rejected, errors, p50_us, p99_us, limiter_p99_us, peak_1s_allowed, is_showcase, note, marker_second, marker_label,
	limit_ratio, cache_hit_pct`

func scanRun(row pgx.Row) (Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.StartedAt, &r.FinishedAt, &r.Status, &r.PlanName, &r.Clients, &r.RPS, &r.DurationS,
		&r.Pattern, &r.Store, &r.Algorithm, &r.LimitDesc, &r.Sent, &r.Allowed, &r.Rejected, &r.Errors,
		&r.P50us, &r.P99us, &r.LimiterP99us, &r.Peak1sAllowed, &r.IsShowcase, &r.Note, &r.MarkerSecond, &r.MarkerLabel,
		&r.LimitRatio, &r.CacheHitPct)
	return r, err
}

func (s *Store) CreateRun(ctx context.Context, r Run) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `INSERT INTO load_runs (plan_name, clients, rps, duration_s, pattern, store, algorithm, limit_desc)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`,
		r.PlanName, r.Clients, r.RPS, r.DurationS, r.Pattern, r.Store, r.Algorithm, r.LimitDesc).Scan(&id)
	return id, err
}

// FinishRun stores totals and per-second rows in one transaction.
func (s *Store) FinishRun(ctx context.Context, r Run, seconds []RunSecond) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `UPDATE load_runs SET status=$2, finished_at=now(), sent=$3, allowed=$4, rejected=$5, errors=$6,
		p50_us=$7, p99_us=$8, limiter_p99_us=$9, peak_1s_allowed=$10,
		limit_ratio=$11, cache_hit_pct=$12 WHERE id=$1`,
		r.ID, r.Status, r.Sent, r.Allowed, r.Rejected, r.Errors, r.P50us, r.P99us, r.LimiterP99us, r.Peak1sAllowed,
		r.LimitRatio, r.CacheHitPct)
	if err != nil {
		return err
	}
	rows := make([][]any, len(seconds))
	for i, sec := range seconds {
		rows[i] = []any{r.ID, sec.Second, sec.Instance, sec.Allowed, sec.Rejected, sec.Errors}
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"run_seconds"},
		[]string{"run_id", "second", "instance", "allowed", "rejected", "errors"}, pgx.CopyFromRows(rows)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) ListRuns(ctx context.Context, limit int) ([]Run, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+runColumns+` FROM load_runs ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) GetRun(ctx context.Context, id int64) (Run, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM load_runs WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// ShowcaseRun returns the pinned run, or the latest finished run if none is pinned.
func (s *Store) ShowcaseRun(ctx context.Context) (Run, error) {
	r, err := scanRun(s.pool.QueryRow(ctx, `SELECT `+runColumns+` FROM load_runs
		WHERE status='done' ORDER BY is_showcase DESC, id DESC LIMIT 1`))
	if errors.Is(err, pgx.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

func (s *Store) RunSeconds(ctx context.Context, runID int64) ([]RunSecond, error) {
	rows, err := s.pool.Query(ctx, `SELECT second, instance, allowed, rejected, errors FROM run_seconds
		WHERE run_id=$1 ORDER BY second, instance`, runID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (RunSecond, error) {
		var r RunSecond
		err := row.Scan(&r.Second, &r.Instance, &r.Allowed, &r.Rejected, &r.Errors)
		return r, err
	})
}

// PinShowcase makes runID the showcase run, unpinning any other.
func (s *Store) PinShowcase(ctx context.Context, runID int64, note string, markerSecond *int, markerLabel string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `UPDATE load_runs SET is_showcase=false WHERE is_showcase`); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE load_runs SET is_showcase=true, note=$2, marker_second=$3, marker_label=$4
		WHERE id=$1 AND status='done'`, runID, note, markerSecond, markerLabel)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return tx.Commit(ctx)
}

// SetRunMarker records the first notable event during a run (for example
// "switched to redis" or "Redis unreachable"), shown as a line on the replay.
func (s *Store) SetRunMarker(ctx context.Context, runID int64, second int, label string) error {
	_, err := s.pool.Exec(ctx, `UPDATE load_runs SET marker_second=$2, marker_label=$3
		WHERE id=$1 AND marker_second IS NULL`, runID, second, label)
	return err
}

// NoteStoreSwitch records a store change made while a run is in progress,
// e.g. "memory" becomes "memory → redis".
func (s *Store) NoteStoreSwitch(ctx context.Context, runID int64, newStore string) error {
	_, err := s.pool.Exec(ctx, `UPDATE load_runs SET store = store || ' → ' || $2
		WHERE id=$1 AND status='running'`, runID, newStore)
	return err
}
