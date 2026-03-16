local master_status_key = KEYS[1]
local queue_key = KEYS[2]
local total_key = KEYS[3]
local generation_key = KEYS[4]

local expected_lock = ARGV[1]
local generation_uuid = ARGV[2]
local total_count = ARGV[3]
local redis_ttl = tonumber(ARGV[4])

-- CAS: verify we still own the lock
if redis.call('get', master_status_key) ~= expected_lock then
  return 0
end

-- Push test IDs to queue (ARGV[5] onwards)
for i = 5, #ARGV do
  redis.call('lpush', queue_key, ARGV[i])
end

-- Set metadata
redis.call('set', total_key, total_count)
redis.call('set', generation_key, generation_uuid)
redis.call('set', master_status_key, 'ready')

-- Apply TTLs
redis.call('expire', queue_key, redis_ttl)
redis.call('expire', total_key, redis_ttl)
redis.call('expire', generation_key, redis_ttl)
redis.call('expire', master_status_key, redis_ttl)

return 1
