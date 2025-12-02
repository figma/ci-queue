# frozen_string_literal: true

module CI
  module Queue
    module Redis
      class UpdateTestDurationMovingAverage
        def initialize(redis, key: 'test_duration_moving_averages', smoothing_factor: 0.2, slow_smoothing_factor: 0.01)
          @redis = redis
          @key = key
          @smoothing_factor = smoothing_factor
          @slow_smoothing_factor = slow_smoothing_factor
        end

        def update_batch(pairs)
          return 0 if pairs.nil? || pairs.empty?

          test_ids = pairs.map(&:first)
          current_values = @redis.hmget(@key, *test_ids)

          writes = []
          pairs.each_with_index do |(test_id, duration), idx|
            current = current_values[idx]
            new_avg = if current
                        current_avg = current.to_f
                        # Use slow smoothing if new duration is faster (shorter), fast smoothing if slower (longer)
                        factor = duration < current_avg ? @slow_smoothing_factor : @smoothing_factor
                        factor * duration + (1 - factor) * current_avg
                      else
                        duration
                      end
            writes << [test_id, new_avg]
          end

          @redis.mapped_hmset(@key, writes.to_h) unless writes.empty?

          writes.size
        end
      end
    end
  end
end
