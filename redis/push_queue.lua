local master_status_key = KEYS[1]
local queue_key = KEYS[2]
local total_key = KEYS[3]
local current_generation_key = KEYS[4]

local expected_lock = ARGV[1]
local generation = ARGV[2]
local total = ARGV[3]
local redis_ttl = tonumber(ARGV[4])

-- Fence a master that resumed after its lease expired and another worker won.
if redis.call('get', master_status_key) ~= expected_lock then
  return 0
end

-- Publishing the queue and changing the status to ready must be atomic. No
-- worker can observe a ready queue before every test has been enqueued.
redis.call('del', queue_key)
for index = 5, #ARGV do
  redis.call('lpush', queue_key, ARGV[index])
end

redis.call('set', total_key, total)
redis.call('set', current_generation_key, generation)
redis.call('set', master_status_key, 'ready')

redis.call('expire', queue_key, redis_ttl)
redis.call('expire', total_key, redis_ttl)
redis.call('expire', current_generation_key, redis_ttl)
redis.call('expire', master_status_key, redis_ttl)

return 1
