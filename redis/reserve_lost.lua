local zset_key = KEYS[1]
local processed_key = KEYS[2]
local worker_queue_key = KEYS[3]
local owners_key = KEYS[4]
local test_group_timeout_key = KEYS[5]

local current_time = tonumber(ARGV[1])
local timeout = tonumber(ARGV[2])
local use_dynamic_deadline = ARGV[3] == "true"
local default_timeout = tonumber(ARGV[4]) or 0

-- Helper: checks if a test can be stolen
-- Returns true if heartbeat is old (> 2 minutes) or missing
-- Owner value format: "worker_queue_key|initial_reservation_time|last_heartbeat_time"
local function can_steal_test(test)
  local owner_value = redis.call('hget', owners_key, test)
  if not owner_value then return true end -- No owner, can steal
  
  local first_pipe = string.find(owner_value, "|")
  if not first_pipe then return true end
  
  local rest = string.sub(owner_value, first_pipe + 1)
  local second_pipe = string.find(rest, "|")
  
  local last_heartbeat
  if second_pipe then
    -- New format: worker_key|initial_time|last_heartbeat
    last_heartbeat = tonumber(string.sub(rest, second_pipe + 1))
  else
    -- Old format: worker_key|timestamp (treat as last heartbeat)
    last_heartbeat = tonumber(rest)
  end
  
  if not last_heartbeat then return true end
  
  local heartbeat_age = current_time - last_heartbeat
  
  -- Only steal if heartbeat is old (> 2 minutes)
  return heartbeat_age >= 120
end

-- Collect tests that can be stolen
local stealable_tests = {}

local all_running_tests = redis.call('zrange', zset_key, 0, -1)
for _, test in ipairs(all_running_tests) do
  if redis.call('sismember', processed_key, test) == 0 then
    if can_steal_test(test) then
      table.insert(stealable_tests, test)
    end
  end
end

for _, test in ipairs(stealable_tests) do
  if redis.call('sismember', processed_key, test) == 0 then
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
    -- Store owner with initial reservation time and last heartbeat time
    local new_owner_value = worker_queue_key .. "|" .. current_time .. "|" .. current_time
    redis.call('hset', owners_key, test, new_owner_value) -- Take ownership
    return test
  end
end

return nil
