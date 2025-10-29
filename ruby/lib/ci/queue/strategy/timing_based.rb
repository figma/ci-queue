# frozen_string_literal: true
require_relative 'base'
require_relative '../redis/exponential_moving_average'
require 'json'

module CI
  module Queue
    module Strategy
      class TimingBased < Base
        def order_tests(tests, random: Random.new, config: nil, redis: nil)
          timing_data = load_timing_data(config, redis)
          fallback_duration = config&.timing_fallback_duration || 100.0

          tests.sort_by do |test|
            duration = timing_data[test.id] || fallback_duration
            -duration # Negative for descending order (longest first)
          end
        end

        private

        def load_timing_data(config, redis)
          timing_data = {}

          # Strategy 1: Try Redis via HSCAN (non-blocking)
          if redis
            begin
              timing_ema = CI::Queue::Redis::ExponentialMovingAverage.new(
                redis,
                hash_key: config&.timing_redis_key || 'timing_data'
              )

              timing_data = timing_ema.load_all(count: config&.timing_hscan_count || 1000)
              puts "Loaded #{timing_data.size} timing entries from Redis via HSCAN" if ENV['VERBOSE']
            rescue ::Redis::BaseError => e
              warn "Warning: Failed to load timing data from Redis: #{e.message}"
            end
          end

          # Strategy 2: Fallback to JSON file if Redis unavailable or empty
          if timing_data.empty? && config&.timing_file && ::File.exist?(config.timing_file)
            begin
              timing_data = JSON.parse(::File.read(config.timing_file))
              puts "Loaded #{timing_data.size} timing entries from file #{config.timing_file}" if ENV['VERBOSE']
            rescue JSON::ParserError => e
              warn "Warning: Could not parse timing file #{config.timing_file}: #{e.message}"
            rescue => e
              warn "Warning: Could not read timing file #{config.timing_file}: #{e.message}"
            end
          end

          timing_data
        end
      end
    end
  end
end
