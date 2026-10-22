-- Fixed window counter, run atomically inside Redis.
--
-- KEYS[1]  window hash, e.g. rl:fw:<rule_id>:<window_ms>:<key_id>
-- ARGV[1]  window length in milliseconds
-- ARGV[2]  limit (requests per window)
--
-- Returns {allowed (0/1), remaining, retry_after_ms, ms_until_window_end}
--
-- The window number is computed from Redis's clock (TIME), not the app's,
-- so instances near a window boundary always agree on which window they're in.
-- The window number is stored inside the hash instead of in the key name,
-- so the script only touches the one key it declares in KEYS.

local window = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local cur = math.floor(now / window)
local reset = (cur + 1) * window - now

local data = redis.call('HMGET', KEYS[1], 'w', 'n')
local count = 0
if tonumber(data[1]) == cur then
  count = tonumber(data[2]) or 0
end

if count >= limit then
  return {0, 0, reset, reset}
end

count = count + 1
-- Setting the value and the expiry in one script means a crash can never
-- leave a counter without an expiry (unlike separate INCR + EXPIRE calls).
redis.call('HSET', KEYS[1], 'w', tostring(cur), 'n', count)
redis.call('PEXPIRE', KEYS[1], reset + 1000)

return {1, limit - count, 0, reset}
