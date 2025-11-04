# frozen_string_literal: true

module CI
  module Queue
    module Redis
      class TestDurationMovingAverages
        def initialize(redis, key: "test_duration_moving_averages")
          @redis = redis
          @key = key
          @loaded = false
        end

        def loaded?
          @loaded
        end

        def [](test_id)
          load_all unless loaded?
          @values[test_id]
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
