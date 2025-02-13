local zset_key = KEYS[1]
local processed_key = KEYS[2]
local worker_queue_key = KEYS[3]
local owners_key = KEYS[4]
local test_group_timeout_key = KEYS[5]

local current_time = ARGV[1]
local timeout = ARGV[2]
local use_dynamic_deadline = ARGV[3] == "true"

local lost_tests
if use_dynamic_deadline then
  lost_tests = redis.call('zrangebyscore', zset_key, 0, current_time)
else
  lost_tests = redis.call('zrangebyscore', zset_key, 0, current_time - timeout)
end

for _, test in ipairs(lost_tests) do
  if redis.call('sismember', processed_key, test) == 0 then
    if use_dynamic_deadline then
      local dynamic_timeout = redis.call('hget', test_group_timeout_key, test)
      redis.call('zadd', zset_key, current_time + dynamic_timeout, test)
    else
      redis.call('zadd', zset_key, current_time, test)
    end
    redis.call('lpush', worker_queue_key, test)
    redis.call('hset', owners_key, test, worker_queue_key) -- Take ownership
    return test
  end
end

return nil
