-- Token bucket, run atomically inside Redis.
--
-- KEYS[1]  bucket hash, e.g. rl:tb:<rule_id>:<key_id>
-- ARGV[1]  refill rate in tokens per millisecond
-- ARGV[2]  capacity (burst)
--
-- Returns {allowed (0/1), remaining tokens, retry_after_ms, ms_until_full}
--
-- Why Lua: reading the bucket, refilling and writing it back must happen as
-- one step. With separate GET and SET calls, two instances could both read
-- "1 token left" and both allow a request.
--
-- Why redis.call('TIME'): every instance uses Redis's clock, so instances
-- whose own clocks drift apart still agree on how much time has passed.
-- Redis 5+ replicates the script's effects (not the script), so calling
-- TIME here is safe for replicas.

local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil or ts == nil then
  -- No bucket (new client, or it expired because it was full): start full.
  tokens = capacity
  ts = now
end

if now > ts then
  tokens = tokens + (now - ts) * rate
  ts = now
end

-- Caps the refill, and clamps old buckets when a rule's capacity is lowered.
if tokens > capacity then
  tokens = capacity
end

local allowed = 0
local retry_after = 0
if tokens >= 1 then
  allowed = 1
  tokens = tokens - 1
else
  retry_after = math.ceil((1 - tokens) / rate)
end

local until_full = math.ceil((capacity - tokens) / rate)

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', tostring(ts))
-- A bucket that has been idle long enough to be full again is identical to a
-- missing bucket, so it can expire. This stops Redis filling up with keys.
redis.call('PEXPIRE', KEYS[1], until_full + 1000)

return {allowed, math.floor(tokens), retry_after, until_full}
