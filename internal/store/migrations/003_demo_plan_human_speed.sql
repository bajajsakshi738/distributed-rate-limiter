-- The demo key is used by the "Try it yourself" button. At 5 requests per
-- second a person clicking can never reach the limit, so make it reachable:
-- 3 requests per 10 seconds (burst 3, then one more every ~3.3 seconds).
UPDATE rules SET limit_count = 3, window_ms = 10000, burst = 3, updated_at = now()
WHERE name = 'demo';
