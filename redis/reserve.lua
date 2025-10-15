local queue_key = KEYS[1]
local zset_key = KEYS[2]
local processed_key = KEYS[3]
local worker_queue_key = KEYS[4]
local owners_key = KEYS[5]
local test_group_timeout_key = KEYS[6]

local current_time = ARGV[1]
local use_dynamic_deadline = ARGV[2] == "true"
local default_timeout = ARGV[3] or 0

local test = redis.call('rpop', queue_key)
if test then
  if use_dynamic_deadline then
    local dynamic_timeout = redis.call('hget', test_group_timeout_key, test)
    if not dynamic_timeout then
      dynamic_timeout = default_timeout
    else
      dynamic_timeout = tonumber(dynamic_timeout)
    end
    redis.call('zadd', zset_key, current_time + dynamic_timeout, test)
  else
    redis.call('zadd', zset_key, current_time, test)
  end
  redis.call('lpush', worker_queue_key, test)
  redis.call('hset', owners_key, test, worker_queue_key)
  return test
else
  return nil
end
