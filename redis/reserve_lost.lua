local zset_key = KEYS[1]
local processed_key = KEYS[2]
local worker_queue_key = KEYS[3]
local owners_key = KEYS[4]
local test_group_timeout_key = KEYS[5]
local heartbeats_key = KEYS[6]

local current_time = tonumber(ARGV[1])
local timeout = tonumber(ARGV[2])
local use_dynamic_deadline = ARGV[3] == "true"
local default_timeout = tonumber(ARGV[4]) or 0
local heartbeat_grace_period = tonumber(ARGV[5]) or 30

local lost_tests
if use_dynamic_deadline then
  lost_tests = redis.call('zrangebyscore', zset_key, 0, current_time)
else
  lost_tests = redis.call('zrangebyscore', zset_key, 0, current_time - timeout)
end

for _, test in ipairs(lost_tests) do
  if redis.call('sismember', processed_key, test) == 0 then
    -- Check if the owner is still actively heartbeating
    local last_heartbeat = redis.call('hget', heartbeats_key, test)
    if last_heartbeat and last_heartbeat ~= "" then
      local heartbeat_age = current_time - tonumber(last_heartbeat)
      -- If heartbeated recently (within grace period), skip this test
      -- The owner is still actively working on it
      if heartbeat_age < heartbeat_grace_period then
        -- Skip this test, try the next one
        -- Don't claim it since the worker is still alive
      else
        -- Heartbeat is stale, safe to claim
        if use_dynamic_deadline then
          local dynamic_timeout = redis.call('hget', test_group_timeout_key, test)
          if not dynamic_timeout or dynamic_timeout == "" then
            dynamic_timeout = default_timeout
          else
            dynamic_timeout = tonumber(dynamic_timeout)
          end
          redis.call('zadd', zset_key, current_time + dynamic_timeout, test)
        else
          redis.call('zadd', zset_key, current_time + timeout, test)
        end
        redis.call('lpush', worker_queue_key, test)
        redis.call('hset', owners_key, test, worker_queue_key) -- Take ownership
        redis.call('hdel', heartbeats_key, test) -- Clear stale heartbeat
        return test
      end
    else
      -- No heartbeat record, proceed with claiming (legacy behavior or never heartbeated)
      if use_dynamic_deadline then
        local dynamic_timeout = redis.call('hget', test_group_timeout_key, test)
        if not dynamic_timeout or dynamic_timeout == "" then
          dynamic_timeout = default_timeout
        else
          dynamic_timeout = tonumber(dynamic_timeout)
        end
        redis.call('zadd', zset_key, current_time + dynamic_timeout, test)
      else
        redis.call('zadd', zset_key, current_time + timeout, test)
      end
      redis.call('lpush', worker_queue_key, test)
      redis.call('hset', owners_key, test, worker_queue_key) -- Take ownership
      return test
    end
  end
end

return nil
