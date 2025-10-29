# frozen_string_literal: true
require_relative 'base'
require_relative '../redis/exponential_moving_average'
require 'json'

module CI
  module Queue
    module Strategy
      class SuiteBinPacking < Base
        def order_tests(tests, random: Random.new, config: nil, redis: nil)
          timing_data = load_timing_data(config, redis)
          pp timing_data if ENV['VERBOSE']
          max_duration = config&.suite_max_duration || 120_000
          fallback_duration = config&.timing_fallback_duration || 100.0
          buffer_percent = config&.suite_buffer_percent || 10

          # Group tests by suite name
          suites = tests.group_by { |test| extract_suite_name(test.id) }

          # Create chunks for each suite
          chunks = []
          suites.each do |suite_name, suite_tests|
            chunks.concat(
              create_chunks_for_suite(
                suite_name,
                suite_tests,
                max_duration,
                buffer_percent,
                timing_data,
                fallback_duration
              )
            )
          end

          # Sort chunks by estimated duration (longest first)
          chunks.sort_by { |chunk| -chunk.estimated_duration }
        end

        private

        def extract_suite_name(test_id)
          test_id.split('#').first
        end

        def load_timing_data(config, redis)
          timing_data = {}

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

          if timing_data.empty? && config&.timing_file && ::File.exist?(config.timing_file)
            begin
              timing_data = JSON.parse(::File.read(config.timing_file))
              puts "Loaded #{timing_data.size} timing entries from file #{config.timing_file}" if ENV['VERBOSE']
            rescue JSON::ParserError => e
              warn "Warning: Could not parse timing file #{config.timing_file}: #{e.message}"
            end
          end

          timing_data
        end

        def get_test_duration(test_id, timing_data, fallback_duration)
          timing_data[test_id]&.to_f || fallback_duration
        end

        def create_chunks_for_suite(suite_name, suite_tests, max_duration, buffer_percent, timing_data, fallback_duration)
          # Calculate total suite duration
          total_duration = suite_tests.sum do |test|
            get_test_duration(test.id, timing_data, fallback_duration)
          end

          # If suite fits in max duration, create full_suite chunk
          if total_duration <= max_duration
            chunk_id = "#{suite_name}:full_suite"
            # Don't store test_ids in Redis - worker will resolve from index
            # But pass test_count for timeout calculation
            return [TestChunk.new(chunk_id, suite_name, :full_suite, [], total_duration, test_count: suite_tests.size)]
          end

          # Suite too large - split into partial_suite chunks
          split_suite_into_chunks(
            suite_name,
            suite_tests,
            max_duration,
            buffer_percent,
            timing_data,
            fallback_duration
          )
        end

        def split_suite_into_chunks(suite_name, suite_tests, max_duration, buffer_percent, timing_data, fallback_duration)
          # Apply buffer to max duration
          effective_max = max_duration * (1 - buffer_percent / 100.0)

          # Sort tests by duration (longest first for better bin packing)
          sorted_tests = suite_tests.sort_by do |test|
            -get_test_duration(test.id, timing_data, fallback_duration)
          end

          # First-fit decreasing bin packing
          chunks = []
          current_chunk_tests = []
          current_chunk_duration = 0.0
          chunk_index = 0

          sorted_tests.each do |test|
            test_duration = get_test_duration(test.id, timing_data, fallback_duration)

            if current_chunk_duration + test_duration > effective_max && current_chunk_tests.any?
              # Finalize current chunk and start new one
              chunk_id = "#{suite_name}:chunk_#{chunk_index}"
              test_ids = current_chunk_tests.map(&:id)
              chunks << TestChunk.new(
                chunk_id,
                suite_name,
                :partial_suite,
                test_ids,
                current_chunk_duration
              )

              current_chunk_tests = [test]
              current_chunk_duration = test_duration
              chunk_index += 1
            else
              current_chunk_tests << test
              current_chunk_duration += test_duration
            end
          end

          # Add final chunk
          if current_chunk_tests.any?
            chunk_id = "#{suite_name}:chunk_#{chunk_index}"
            test_ids = current_chunk_tests.map(&:id)
            chunks << TestChunk.new(
              chunk_id,
              suite_name,
              :partial_suite,
              test_ids,
              current_chunk_duration
            )
          end

          chunks
        end

        private def load_timing_data_from_redis
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
        end
      end
    end
  end
end
