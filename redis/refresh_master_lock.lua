local master_status_key = KEYS[1]
local expected_value = ARGV[1]
local ttl = ARGV[2]

if redis.call('GET', master_status_key) == expected_value then
  redis.call('SET', master_status_key, expected_value, 'EX', ttl)
  return 1
end
return 0
