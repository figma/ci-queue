# frozen_string_literal: true

require_relative 'base'
require 'json'

module CI
  module Queue
    module Strategy
      class SuiteBinPacking < Base
        class << self
          def load_timing_data(file_path)
            return {} unless file_path && ::File.exist?(file_path)

            JSON.parse(::File.read(file_path))
          rescue JSON::ParserError => e
            warn "Warning: Could not parse timing file #{file_path}: #{e.message}"
            {}
          end
        end

        def initialize(config, redis: nil)
          super(config)

          @moving_average = CI::Queue::Redis::TestDurationMovingAverages.new(redis) if redis

          @timing_data = if config&.timing_file
                           self.class.load_timing_data(config.timing_file)
                         else
                           {}
                         end
          # Enforce the max chunk duration falls within this range. 
          @minimum_max_duration = config&.suite_minimum_max_chunk_duration || 120_000
          @maximum_max_duration = config&.suite_maximum_max_chunk_duration || 300_000
          @fallback_duration = config&.timing_fallback_duration || 100.0
          @buffer_percent = config&.suite_buffer_percent || 10

          # Cache for test durations to avoid redundant lookups
          @duration_cache = {}
        end

        def order_tests(tests, random: ::Random.new, redis: nil)
          # Clear duration cache for this ordering run
          @duration_cache.clear

          # Calculate dynamic max_duration based on total duration and parallelism
          dynamic_max_duration = calculate_dynamic_max_duration(tests)

          # Group tests by suite name
          suites = tests.group_by { |test| extract_suite_name(test.id) }

          # Create chunks for each suite using dynamic max_duration
          chunks = []
          suites.each do |suite_name, suite_tests|
            chunks.concat(
              create_chunks_for_suite(
                suite_name,
                suite_tests,
                dynamic_max_duration
              )
            )
          end

          # Sort chunks by estimated duration (longest first)
          chunks.sort_by { |chunk| -chunk.estimated_duration }
        end

        private

        def extract_suite_name(test_id)
          suite_name = test_id.split('#').first

          # if the test were a spec style test, we would need to extract the suite name from the first part again
          if suite_name.include?('::')
            suite_name.split('::').first
          else
            suite_name
          end
        end

        def get_test_duration(test_id)
          # Return cached value if available
          return @duration_cache[test_id] if @duration_cache.key?(test_id)

          duration = if @moving_average
                       avg = @moving_average[test_id]
                       if avg
                         avg
                       elsif @timing_data.key?(test_id)
                         @timing_data[test_id]
                       else
                         @fallback_duration
                       end
                     elsif @timing_data.key?(test_id)
                       @timing_data[test_id]
                     else
                       @fallback_duration
                     end

          # Cache the result
          @duration_cache[test_id] = duration
          duration
        end

        def calculate_dynamic_max_duration(tests)
          # Get parallel job count from environment variable
          parallel_job_count = ENV['BUILDKITE_PARALLEL_JOB_COUNT']&.to_i

          puts "parallel_job_count: #{parallel_job_count}"

          # If no parallel job count, fall back to configured minimum max_duration
          return @minimum_max_duration unless parallel_job_count && parallel_job_count > 0

          # Calculate total duration of all tests
          total_duration = tests.sum do |test|
            get_test_duration(test.id)
          end

          # Calculate max_duration per chunk: total_duration / parallel_job_count
          # This gives us the target max chunk time
          base_max_duration = total_duration.to_f / parallel_job_count

          puts "base_max_duration: #{base_max_duration}, @minimum_max_duration: #{@minimum_max_duration}, @maximum_max_duration: #{@maximum_max_duration}"

          # Ensure we don't go above or below reasonable floor values.
          # Use configured max_duration as a floor to prevent extremely small chunks
          max_duration = [base_max_duration, @maximum_max_duration].min
          [max_duration, @minimum_max_duration].max
        end

        def create_chunks_for_suite(suite_name, suite_tests, max_duration)
          split_suite_into_chunks(suite_name, suite_tests, max_duration)
        end

        def split_suite_into_chunks(suite_name, suite_tests, max_duration)
          # Apply buffer to max duration
          effective_max = max_duration * (1 - @buffer_percent / 100.0)

          # Preserve original test order (as they appear in the file)
          # First-fit bin packing (preserving order)
          chunks = []
          current_chunk_tests = []
          current_chunk_duration = 0.0
          chunk_index = 0

          suite_tests.each do |test|
            test_duration = get_test_duration(test.id)

            if current_chunk_duration + test_duration > effective_max && current_chunk_tests.any?
              # Finalize current chunk and start new one
              chunk_id = "#{suite_name}:chunk_#{chunk_index}"
              test_ids = current_chunk_tests.map(&:id)
              chunks << TestChunk.new(
                chunk_id,
                suite_name,
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
              test_ids,
              current_chunk_duration
            )
          end

          chunks
        end
      end
    end
  end
end
