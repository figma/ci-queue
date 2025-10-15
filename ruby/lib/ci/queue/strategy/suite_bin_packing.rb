# frozen_string_literal: true
require_relative 'base'
require 'json'

module CI
  module Queue
    module Strategy
      class SuiteBinPacking < Base
        def order_tests(tests, random: Random.new, config: nil)
          timing_data = load_timing_data(config&.timing_file)
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

        def load_timing_data(file_path)
          return {} unless file_path && ::File.exist?(file_path)

          JSON.parse(::File.read(file_path))
        rescue JSON::ParserError => e
          warn "Warning: Could not parse timing file #{file_path}: #{e.message}"
          {}
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
            # Don't store test_ids - worker will resolve from index
            return [TestChunk.new(chunk_id, suite_name, :full_suite, [], total_duration)]
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
      end
    end
  end
end
