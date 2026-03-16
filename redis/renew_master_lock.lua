local master_status_key = KEYS[1]
local expected_value = ARGV[1]
local ttl = tonumber(ARGV[2])

if redis.call('get', master_status_key) == expected_value then
  redis.call('expire', master_status_key, ttl)
  return 1
else
  return 0
end
