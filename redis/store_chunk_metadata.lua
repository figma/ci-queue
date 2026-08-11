local master_status_key = KEYS[1]
local chunks_key = KEYS[2]
local test_group_timeout_key = KEYS[3]

local expected_lock = ARGV[1]
local master_lock_ttl = tonumber(ARGV[2])
local redis_ttl = tonumber(ARGV[3])

-- Metadata writes are fenced too: an expired master must not overwrite the
-- metadata prepared by its replacement.
if redis.call('get', master_status_key) ~= expected_lock then
  return 0
end

local argument_index = 4
local chunk_key_index = 4
while argument_index <= #ARGV do
  local chunk_id = ARGV[argument_index]
  local chunk_json = ARGV[argument_index + 1]
  local chunk_timeout = ARGV[argument_index + 2]
  local chunk_key = KEYS[chunk_key_index]

  redis.call('set', chunk_key, chunk_json)
  redis.call('expire', chunk_key, redis_ttl)
  redis.call('sadd', chunks_key, chunk_id)
  redis.call('hset', test_group_timeout_key, chunk_id, chunk_timeout)

  argument_index = argument_index + 3
  chunk_key_index = chunk_key_index + 1
end

redis.call('expire', chunks_key, redis_ttl)
redis.call('expire', test_group_timeout_key, redis_ttl)
redis.call('expire', master_status_key, master_lock_ttl)

return 1
