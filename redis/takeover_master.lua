-- Atomically attempt to take over as master when current master is dead during setup
-- Returns 1 if takeover succeeded, 0 otherwise

local master_status_key = KEYS[1]
local master_worker_id_key = KEYS[2]
local master_setup_heartbeat_key = KEYS[3]

local new_worker_id = ARGV[1]
local current_time = tonumber(ARGV[2])
local heartbeat_timeout = tonumber(ARGV[3])
local redis_ttl = tonumber(ARGV[4])

-- Step 1: Verify status is still 'setup'
local status = redis.call('get', master_status_key)
if status ~= 'setup' then
  return 0
end

-- Step 2: Check if heartbeat is stale or missing
local last_heartbeat = redis.call('get', master_setup_heartbeat_key)
if last_heartbeat then
  local heartbeat_age = current_time - tonumber(last_heartbeat)
  if heartbeat_age < heartbeat_timeout then
    -- Master is still alive, heartbeat is fresh
    return 0
  end
end
-- If no heartbeat exists and status is 'setup', master may have died before first heartbeat
-- Allow takeover in this case (heartbeat_timeout acts as grace period for initial setup)

-- Step 3: Delete old master-status to allow SETNX
redis.call('del', master_status_key)

-- Step 4: Atomically try to claim master role
local claimed = redis.call('setnx', master_status_key, 'setup')
if claimed == 0 then
  -- Another worker beat us to it
  return 0
end

-- Step 5: We got master role - update worker ID and heartbeat
redis.call('set', master_worker_id_key, new_worker_id)
redis.call('set', master_setup_heartbeat_key, current_time)

-- Set TTLs
redis.call('expire', master_status_key, redis_ttl)
redis.call('expire', master_worker_id_key, redis_ttl)
redis.call('expire', master_setup_heartbeat_key, redis_ttl)

return 1
