local zset_key = KEYS[1]
local processed_key = KEYS[2]
local owners_key = KEYS[3]
local worker_queue_key = KEYS[4]
local test_group_timeout_key = KEYS[5]

local current_time = tonumber(ARGV[1])
local test = ARGV[2]
local default_timeout = tonumber(ARGV[3]) or 0

-- already processed, we do not need to bump the timestamp
if redis.call('sismember', processed_key, test) == 1 then
  return false
end

-- we're still the owner of the test, check if we need to extend the deadline
local owner_value = redis.call('hget', owners_key, test)
if owner_value then
  -- Parse owner value: format is "worker_queue_key|initial_reservation_time|last_heartbeat_time"
  local first_pipe = string.find(owner_value, "|")
  if not first_pipe then
    return false
  end
  local stored_worker_key = string.sub(owner_value, 1, first_pipe - 1)
  
  if stored_worker_key == worker_queue_key then
    -- Parse initial reservation time and last heartbeat time
    local rest = string.sub(owner_value, first_pipe + 1)
    local second_pipe = string.find(rest, "|")
    local initial_reservation_time
    if second_pipe then
      initial_reservation_time = tonumber(string.sub(rest, 1, second_pipe - 1))
    else
      -- Backward compatibility: old format only has one timestamp
      initial_reservation_time = tonumber(rest)
    end
    
    -- Update last heartbeat timestamp in owners hash (keep initial reservation time)
    local new_owner_value = worker_queue_key .. "|" .. (initial_reservation_time or current_time) .. "|" .. current_time
    redis.call('hset', owners_key, test, new_owner_value)
    
    local deadline = redis.call('zscore', zset_key, test)
    if deadline then
      deadline = tonumber(deadline)
      
      -- Get the estimated timeout for this test
      local estimated_timeout = redis.call('hget', test_group_timeout_key, test)
      if not estimated_timeout or estimated_timeout == "" then
        estimated_timeout = default_timeout
      else
        estimated_timeout = tonumber(estimated_timeout)
      end
      
      -- Cap deadline at 3x the estimated timeout from initial reservation
      local max_deadline = (initial_reservation_time or current_time) + (estimated_timeout * 3)
      
      -- Only extend if deadline is within 20 seconds of expiring
      if deadline - 20 < current_time then
        -- Extend by 1 minute, but don't exceed max deadline
        local new_deadline = math.min(current_time + 60, max_deadline)
        
        -- Only update if we're actually extending
        if new_deadline > deadline then
          redis.call('zadd', zset_key, new_deadline, test)
          return {deadline, new_deadline}
        end
      end
    end
    -- No extension needed, but heartbeat was recorded
    return 0
  end
end

return false
