-- A tiny plan for the public demo key shown on the live page, so visitors
-- can trigger 429s themselves with a curl loop.
INSERT INTO rules (name, algorithm, limit_count, window_ms, burst) VALUES ('demo', 'token_bucket', 5, 1000, 5)
    ON CONFLICT (name) DO NOTHING;

INSERT INTO plans (name, rule_id)
    SELECT 'demo', id FROM rules WHERE name = 'demo'
    ON CONFLICT (name) DO NOTHING;
