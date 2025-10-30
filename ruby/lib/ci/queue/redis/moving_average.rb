# frozen_string_literal: true

module CI
  module Queue
    module Redis
      # Represents a redis hash of moving averages for test durations
      #
      # Moving average is calculated using exponential moving average formula
      class MovingAverage
        LUA_SCRIPT= <<~LUA
          local hash_key = KEYS[1]
          local test_id = ARGV[1]
          local new_duration = tonumber(ARGV[2])
          local smoothing = tonumber(ARGV[3])
          local current_avg = redis.call('HGET', hash_key, test_id)
          if current_avg then
            current_avg = tonumber(current_avg)
            local new_avg = smoothing * new_duration + (1 - smoothing) * current_avg
            redis.call('HSET', hash_key, test_id, new_avg)
            return tostring(new_avg)
          else
            redis.call('HSET', hash_key, test_id, new_duration)
            return tostring(new_duration)
          end
        LUA

        def initialize(redis, key: "test_duration_moving_averages", smoothing_factor: 0.2)
          @redis = redis
          @key = key
          @smoothing_factor = smoothing_factor
          @values = {}
        end

        def [](test_id)
          load_all if @values.empty?
          @values[test_id]
        end

        def update(test_id, duration)
          new_avg = @redis.eval(LUA_SCRIPT, keys: [@key], argv: [test_id, duration, @smoothing_factor])
          @values[test_id] = new_avg.to_f
          new_avg.to_f
        end

        def load_all
          batch_size = 1000
          cursor = '0'
          @values = {}

          loop do
            cursor, batch = @redis.hscan(@key, cursor, count: batch_size)
            batch.each do |test_id, value|
              @values[test_id] = value.to_f
            end
            break if cursor == '0'
          end
        end

        def size
          @redis.hlen(@key)
        end
      end
    end
  end
end
