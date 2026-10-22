-- Two results saved with each load test so a replay can show them:
-- how much got through compared with the limit (1.0 = exactly the limit),
-- and how many API key lookups were answered from the in-memory cache.
ALTER TABLE load_runs ADD COLUMN limit_ratio double precision;
ALTER TABLE load_runs ADD COLUMN cache_hit_pct double precision;
