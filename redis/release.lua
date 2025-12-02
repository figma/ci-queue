local zset_key = KEYS[1]
local worker_queue_key = KEYS[2]
local owners_key = KEYS[3]

-- owned_tests = {"SomeTest", "worker:1|1234567890", "SomeOtherTest", "worker:2|1234567891", ...}
local owned_tests = redis.call('hgetall', owners_key)
for index, owner_or_test in ipairs(owned_tests) do
  -- Parse owner value: format is "worker_queue_key|heartbeat_timestamp"
  local pipe_pos = string.find(owner_or_test, "|")
  if pipe_pos then
    local stored_worker_key = string.sub(owner_or_test, 1, pipe_pos - 1)
    
    if stored_worker_key == worker_queue_key then -- If we owned a test
      local test = owned_tests[index - 1]
      redis.call('zadd', zset_key, "0", test) -- We expire the lease immediately
      redis.call('hdel', owners_key, test) -- Remove from owners hash to clear heartbeat
      return nil
    end
  end
end

return nil
