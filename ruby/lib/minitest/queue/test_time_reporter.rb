# frozen_string_literal: true
require 'minitest/reporters'
require 'json'

module Minitest
  module Queue
    class TestTimeReporter < Minitest::Reporters::BaseReporter
      include ::CI::Queue::OutputHelpers

      def initialize(build:, limit: nil, percentile: nil, export_file: nil, **options)
        super(options)
        @test_time_hash = build.fetch
        @limit = limit
        @percentile = percentile
        @export_file = export_file
        @build = build
        @success = true
      end

      def report
        export_timing_data if @export_file

        return if limit.nil? || test_time_hash.empty?

        puts '+++ Test Time Report'

        if offending_tests.empty?
          msg = "The #{humanized_percentile} of test execution time is within #{limit} milliseconds."
          puts green(msg)
          return
        end

        @success = false
        puts <<~EOS
          #{red("Detected #{offending_tests.size} test(s) over the desired time limit.")}
          Please make them faster than #{limit}ms in the #{humanized_percentile} percentile.
        EOS
        offending_tests.each do |test_name, duration|
          puts "#{red(test_name)}: #{duration}ms"
        end
      end

      def success?
        @success
      end

      def record(*)
        raise NotImplementedError
      end

      private

      attr_reader :test_time_hash, :limit, :percentile

      def export_timing_data
        timing_data = {}

        # Try to export from Redis EMA first (preferred)
        if @build.respond_to?(:redis) && @build.respond_to?(:config)
          begin
            require 'ci/queue/redis/exponential_moving_average'
            timing_ema = CI::Queue::Redis::ExponentialMovingAverage.new(
              @build.redis,
              hash_key: @build.config.timing_redis_key || 'timing_data'
            )
            timing_data = timing_ema.export_all

            unless timing_data.empty?
              puts "Exported #{timing_data.size} timing entries from Redis EMA to #{@export_file}"
            end
          rescue ::Redis::BaseError => e
            warn "Warning: Failed to export from Redis EMA: #{e.message}, falling back to list data"
          rescue => e
            # Silently fall through to list-based export if EMA is not available
          end
        end

        # Fallback: export from test_time_hash (list-based data) if Redis EMA unavailable or empty
        if timing_data.empty? && !test_time_hash.empty?
          # Convert test_time_hash to simple format: {"TestClass#method": avg_duration_ms}
          timing_data = test_time_hash.transform_values do |durations|
            durations.sum.to_f / durations.size  # Average duration
          end
          puts "Exported timing data for #{timing_data.size} tests to #{@export_file}"
        end

        return if timing_data.empty?

        File.write(@export_file, JSON.pretty_generate(timing_data))
      rescue => e
        puts "Warning: Failed to export timing data to #{@export_file}: #{e.message}"
      end

      def humanized_percentile
        percentile_in_percentage = percentile * 100
        "#{percentile_in_percentage.to_i}th"
      end

      def offending_tests
        @offending_tests ||= begin
          test_time_hash.each_with_object({}) do |(test_name, durations), offenders|
            duration = calculate_percentile(durations)
            next if duration <= limit
            offenders[test_name] = duration
          end
        end
      end

      def calculate_percentile(array)
        array.sort[(percentile * array.length).ceil - 1]
      end
    end
  end
end
