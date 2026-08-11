local master_status_key = KEYS[1]
local expected_lock = ARGV[1]
local lock_ttl = tonumber(ARGV[2])

if redis.call('get', master_status_key) ~= expected_lock then
  return 0
end

redis.call('expire', master_status_key, lock_ttl)
return 1
