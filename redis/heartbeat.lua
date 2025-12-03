local zset_key = KEYS[1]
local processed_key = KEYS[2]
local owners_key = KEYS[3]
local worker_queue_key = KEYS[4]
local heartbeats_key = KEYS[5]
local test_group_timeout_key = KEYS[6]

local current_time = tonumber(ARGV[1])
local test = ARGV[2]
local default_timeout = tonumber(ARGV[3]) or 0

-- already processed, we do not need to bump the timestamp
if redis.call('sismember', processed_key, test) == 1 then
  return nil
end

-- we're still the owner of the test, we can bump the timestamp
if redis.call('hget', owners_key, test) == worker_queue_key then
  -- Record last heartbeat time in a separate hash for "recent activity" tracking
  redis.call('hset', heartbeats_key, test, current_time)

  -- Get the dynamic timeout for this test (if any) or use default
  local dynamic_timeout = redis.call('hget', test_group_timeout_key, test)
  local timeout_to_use
  if dynamic_timeout and dynamic_timeout ~= "" then
    timeout_to_use = tonumber(dynamic_timeout)
  else
    timeout_to_use = default_timeout
  end

  local new_deadline = current_time + timeout_to_use

  -- Extend the deadline by setting score to current_time + timeout
  redis.call('zadd', zset_key, new_deadline, test)

  -- Return the new deadline and timeout used for logging
  return { new_deadline, timeout_to_use }
end

return nil
