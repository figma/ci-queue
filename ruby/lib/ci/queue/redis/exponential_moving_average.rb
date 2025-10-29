# frozen_string_literal: true

module CI
  module Queue
    module Redis
      class ExponentialMovingAverage
        LUA_SCRIPT= <<~LUA
          local hash_key = KEYS[1]
          local test_id = ARGV[1]
          local new_duration = tonumber(ARGV[2])
          local alpha = tonumber(ARGV[3])

          local current_avg = redis.call('HGET', hash_key, test_id)

          if current_avg then
            current_avg = tonumber(current_avg)
            local new_avg = alpha * new_duration + (1 - alpha) * current_avg
            redis.call('HSET', hash_key, test_id, new_avg)
            return new_avg
          else
            redis.call('HSET', hash_key, test_id, new_duration)
            return new_duration
          end
        LUA

        attr_reader :redis, :hash_key, :alpha

        def initialize(redis, hash_key: 'timing_data', alpha: 0.2)
          @redis = redis
          @hash_key = hash_key
          @alpha = alpha
        end

        def update(test_id, duration)
          redis.eval(
            LUA_SCRIPT,
            keys: [hash_key],
            argv: [test_id, duration.to_f, alpha]
          )
        end

        def load_all(count: 1000)
          timing_data = {}
          cursor = '0'

          loop do
            cursor, fields = redis.hscan(hash_key, cursor, count: count)

            # fields is array: ["key1", "val1", "key2", "val2", ...]
            # Convert to hash and parse floats
            Hash[*fields].each do |test_id, duration_str|
              timing_data[test_id] = duration_str.to_f
            end

            break if cursor == '0'
          end

          timing_data
        end

        def import(timing_hash)
          return if timing_hash.nil? || timing_hash.empty?

          # HMSET expects flattened array: ["key1", "val1", "key2", "val2", ...]
          flattened = timing_hash.to_a.flatten
          redis.hmset(hash_key, *flattened)
        end

        def export_all
          load_all
        end

        def exists?
          redis.exists?(hash_key) && redis.hlen(hash_key) > 0
        end

        def size
          redis.hlen(hash_key)
        end
      end
    end
  end
end
