local master_status_key = KEYS[1]
local queue_key = KEYS[2]
local total_key = KEYS[3]
local current_generation_key = KEYS[4]

local expected_lock_value = ARGV[1]
local total_count = ARGV[2]
local generation_uuid = ARGV[3]
local redis_ttl = ARGV[4]

if redis.call('GET', master_status_key) ~= expected_lock_value then
  return 0
end

if #ARGV > 4 then
  local tests = {}
  for i = 5, #ARGV do
    tests[#tests + 1] = ARGV[i]
  end
  redis.call('LPUSH', queue_key, unpack(tests))
end

redis.call('SET', total_key, total_count)
redis.call('SET', master_status_key, 'ready')
redis.call('SET', current_generation_key, generation_uuid)

redis.call('EXPIRE', queue_key, redis_ttl)
redis.call('EXPIRE', total_key, redis_ttl)
redis.call('EXPIRE', master_status_key, redis_ttl)
redis.call('EXPIRE', current_generation_key, redis_ttl)

return 1
