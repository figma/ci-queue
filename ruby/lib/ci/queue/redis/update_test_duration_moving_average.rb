# frozen_string_literal: true

module CI
  module Queue
    module Redis
      class UpdateTestDurationMovingAverage
        LUA_SCRIPT_BATCH = <<~LUA
          local hash_key = KEYS[1]
          local smoothing = tonumber(ARGV[1])

          for i = 2, #ARGV, 2 do
            local test_id = ARGV[i]
            local new_duration = tonumber(ARGV[i+1])
            local current_avg = redis.call('HGET', hash_key, test_id)

            if current_avg then
              current_avg = tonumber(current_avg)
              local new_avg = smoothing * new_duration + (1 - smoothing) * current_avg
              redis.call('HSET', hash_key, test_id, new_avg)
            else
              redis.call('HSET', hash_key, test_id, new_duration)
            end
          end

          return 'OK'
        LUA

        def initialize(redis, key: "test_duration_moving_averages", smoothing_factor: 0.2)
          @redis = redis
          @key = key
          @smoothing_factor = smoothing_factor
        end

        def update_batch(pairs)
          return 0 if pairs.nil? || pairs.empty?
          argv = [@smoothing_factor] + pairs.flat_map { |test_id, duration| [test_id, duration] }
          @redis.eval(LUA_SCRIPT_BATCH, keys: [@key], argv: argv)
        end
      end
    end
  end
end
