# frozen_string_literal: true
require 'json'

module CI
  module Queue
    class Configuration
      attr_accessor :timeout, :worker_id, :max_requeues, :grind_count, :failure_file, :export_flaky_tests_file
      attr_accessor :requeue_tolerance, :namespace, :failing_test, :statsd_endpoint
      attr_accessor :max_test_duration, :max_test_duration_percentile, :track_test_duration
      attr_accessor :max_test_failed, :redis_ttl
      attr_accessor :strategy, :timing_file, :timing_fallback_duration, :export_timing_file
      attr_accessor :suite_max_duration, :suite_buffer_percent
      attr_accessor :branch
      attr_accessor :timing_redis_url
      attr_accessor :write_duration_averages
      attr_reader :circuit_breakers
      attr_writer :seed, :build_id
      attr_writer :queue_init_timeout, :report_timeout, :inactive_workers_timeout

      class << self
        def from_env(env)
          new(
            build_id: env['CIRCLE_BUILD_URL'] || env['BUILDKITE_BUILD_ID'] || env['TRAVIS_BUILD_ID'] || env['HEROKU_TEST_RUN_ID'] || env['SEMAPHORE_PIPELINE_ID'],
            worker_id: env['CIRCLE_NODE_INDEX'] || env['BUILDKITE_PARALLEL_JOB'] || env['CI_NODE_INDEX'] || env['SEMAPHORE_JOB_ID'],
            seed: env['CIRCLE_SHA1'] || env['BUILDKITE_COMMIT'] || env['TRAVIS_COMMIT'] || env['HEROKU_TEST_RUN_COMMIT_VERSION'] || env['SEMAPHORE_GIT_SHA'],
            flaky_tests: load_flaky_tests(env['CI_QUEUE_FLAKY_TESTS']),
            statsd_endpoint: env['CI_QUEUE_STATSD_ADDR'],
            redis_ttl: env['CI_QUEUE_REDIS_TTL']&.to_i ||  8 * 60 * 60,
            known_flaky_tests: load_known_flaky_tests(env['CI_QUEUE_KNOWN_FLAKY_TESTS']),
            branch: env['BUILDKITE_BRANCH'],
          )
        end

        def load_flaky_tests(path)
          return [] unless path
          ::File.readlines(path).map(&:chomp).to_set
        rescue SystemCallError
          []
        end

        def load_known_flaky_tests(path)
          if path == nil
            return []
          end
          json_data = JSON.parse(::File.read(path))
          known_flaky_test_ids = json_data.map { |test| "#{test['testSuite']}##{test['testName']}" }
          puts "ci-queue: Loaded #{known_flaky_test_ids.size} known flaky tests. These will be skipped from requeueing"
          known_flaky_test_ids.to_set
        rescue SystemCallError, JSON::ParserError, TypeError
          []
        end
      end

      def initialize(
        timeout: 30, build_id: nil, worker_id: nil, max_requeues: 0, requeue_tolerance: 0,
        namespace: nil, seed: nil, flaky_tests: [], statsd_endpoint: nil, max_consecutive_failures: nil,
        grind_count: nil, max_duration: nil, failure_file: nil, max_test_duration: nil,
        max_test_duration_percentile: 0.5, track_test_duration: false, max_test_failed: nil,
        queue_init_timeout: nil, redis_ttl: 8 * 60 * 60, report_timeout: nil, inactive_workers_timeout: nil,
        export_flaky_tests_file: nil, known_flaky_tests: [],
        strategy: :random, timing_file: nil, timing_fallback_duration: 100.0, export_timing_file: nil,
        suite_max_duration: 120_000, suite_buffer_percent: 10,
        branch: nil,
        timing_redis_url: nil
      )
        @build_id = build_id
        @circuit_breakers = [CircuitBreaker::Disabled]
        @failure_file = failure_file
        @flaky_tests = flaky_tests
        @known_flaky_tests = known_flaky_tests
        @grind_count = grind_count
        @max_requeues = max_requeues
        @max_test_duration = max_test_duration
        @max_test_duration_percentile = max_test_duration_percentile
        @max_test_failed = max_test_failed
        @namespace = namespace
        @requeue_tolerance = requeue_tolerance
        @seed = seed
        @statsd_endpoint = statsd_endpoint
        @timeout = timeout
        @queue_init_timeout = queue_init_timeout
        @track_test_duration = track_test_duration
        @worker_id = worker_id
        self.max_consecutive_failures = max_consecutive_failures
        self.max_duration = max_duration
        @redis_ttl = redis_ttl
        @report_timeout = report_timeout
        @inactive_workers_timeout = inactive_workers_timeout
        @export_flaky_tests_file = export_flaky_tests_file
        @strategy = strategy
        @timing_file = timing_file
        @timing_fallback_duration = timing_fallback_duration
        @export_timing_file = export_timing_file
        @suite_max_duration = suite_max_duration
        @suite_buffer_percent = suite_buffer_percent
        @branch = branch
        @timing_redis_url = timing_redis_url
        @write_duration_averages = false
      end

      def queue_init_timeout
        @queue_init_timeout || timeout
      end

      def report_timeout
        @report_timeout || timeout
      end

      def inactive_workers_timeout
        @inactive_workers_timeout || timeout
      end

      def max_consecutive_failures=(max)
        if max
          @circuit_breakers << CircuitBreaker::MaxConsecutiveFailures.new(max_consecutive_failures: max)
        end
      end

      def max_duration=(duration)
        if duration
          @circuit_breakers << CircuitBreaker::Timeout.new(duration: duration)
        end
      end

      def flaky?(test)
        @flaky_tests.include?(test.id)
      end

      def known_flaky?(id)
        @known_flaky_tests.include?(id)
      end

      def seed
        @seed || build_id
      end

      def build_id
        if namespace
          "#{namespace}:#{@build_id}"
        else
          @build_id
        end
      end

      def global_max_requeues(tests_count)
        (tests_count * Float(requeue_tolerance)).ceil
      end
    end
  end
end
