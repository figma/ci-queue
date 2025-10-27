# frozen_string_literal: true
require 'shellwords'
require 'minitest'
require 'minitest/reporters'

require 'minitest/queue/failure_formatter'
require 'minitest/queue/error_report'
require 'minitest/queue/local_requeue_reporter'
require 'minitest/queue/build_status_recorder'
require 'minitest/queue/build_status_reporter'
require 'minitest/queue/order_reporter'
require 'minitest/queue/junit_reporter'
require 'minitest/queue/test_data_reporter'
require 'minitest/queue/grind_recorder'
require 'minitest/queue/grind_reporter'
require 'minitest/queue/test_time_recorder'
require 'minitest/queue/test_time_reporter'

module Minitest
  class Requeue < Skip
    attr_reader :failure

    def initialize(failure)
      super()
      @failure = failure
    end

    def result_label
      "Requeued"
    end

    def backtrace
      failure.backtrace
    end

    def error
      failure.error
    end

    def message
      failure.message
    end
  end

  class Flaked < Skip
    attr_reader :failure

    def initialize(failure)
      super()
      @failure = failure
    end

    def result_label
      "Flaked"
    end

    def backtrace
      failure.backtrace
    end

    def error
      failure.error
    end

    def message
      failure.message
    end
  end

  module Requeueing
    # Make requeues acts as skips for reporters not aware of the difference.
    def skipped?
      super || requeued?
    end

    def requeued?
      Requeue === failure
    end

    def requeue!
      self.failures.unshift(Requeue.new(self.failures.shift))
    end
  end

  module Flakiness
    # Make failed flaky tests acts as skips for reporters not aware of the difference.
    def skipped?
      super || flaked?
    end

    def flaked?
      @flaky ||= false
      !!((Flaked === failure) || @flaky)
    end

    def mark_as_flaked!
      if passed?
        @flaky = true
      else
        self.failures.unshift(Flaked.new(self.failures.shift))
      end
    end
  end

  module WithTimestamps
    attr_accessor :start_timestamp, :finish_timestamp
  end

  module Queue
    attr_writer :run_command_formatter, :project_root

    def run_command_formatter
      @run_command_formatter ||= if defined?(Rails) && defined?(Rails::TestUnitRailtie)
        RAILS_RUN_COMMAND_FORMATTER
      else
        DEFAULT_RUN_COMMAND_FORMATTER
      end
    end

    DEFAULT_RUN_COMMAND_FORMATTER = lambda do |runnable|
      filename = Minitest::Queue.relative_path(runnable.source_location[0])
      identifier = "#{runnable.klass}##{runnable.name}"
      ['bundle', 'exec', 'ruby', '-Ilib:test', filename, '-n', identifier]
    end

    RAILS_RUN_COMMAND_FORMATTER = lambda do |runnable|
      filename = Minitest::Queue.relative_path(runnable.source_location[0])
      lineno = runnable.source_location[1]
      ['bin/rails', 'test', "#{filename}:#{lineno}"]
    end

    def run_command_for_runnable(runnable)
      command = run_command_formatter.call(runnable)
      if command.is_a?(Array)
        Shellwords.join(command)
      else
        command
      end
    end

    def self.project_root
      @project_root ||= Dir.pwd
    end

    def self.relative_path(path, root: project_root)
      Pathname(path).relative_path_from(Pathname(root)).to_s
    rescue ArgumentError, TypeError
      path
    end

    class SingleExample

      def initialize(runnable, method_name)
        @runnable = runnable
        @method_name = method_name
      end

      def id
        @id ||= "#{@runnable}##{@method_name}"
      end

      def <=>(other)
        id <=> other.id
      end

      def with_timestamps
        start_timestamp = current_timestamp
        result = yield
        result
      ensure
        if result
          result.start_timestamp = start_timestamp
          result.finish_timestamp = current_timestamp
        end
      end

      def run
        with_timestamps do
          Minitest.run_one_method(@runnable, @method_name)
        end
      end

      def flaky?
        Minitest.queue.flaky?(self)
      end

      private

      def current_timestamp
        CI::Queue.time_now.to_i
      end
    end

    attr_accessor :queue

    def queue_reporters=(reporters)
      @queue_reporters ||= []
      Reporters.use!(((Reporters.reporters || []) - @queue_reporters) + reporters)
      Minitest.backtrace_filter.add_filter(%r{exe/minitest-queue|lib/ci/queue/})
      @queue_reporters = reporters
    end

    def loaded_tests
      Minitest::Test.runnables.flat_map do |runnable|
        runnable.runnable_methods.map do |method_name|
          SingleExample.new(runnable, method_name)
        end
      end
    end

    def __run(*args)
      if queue
        run_from_queue(*args)

        if queue.config.circuit_breakers.any?(&:open?)
          STDERR.puts queue.config.circuit_breakers.map(&:message).join(' ').strip
        end

        if queue.max_test_failed?
          STDERR.puts 'This worker is exiting early because too many failed tests were encountered.'
        end
      else
        super
      end
    end

    def run_from_queue(reporter, *)
      queue.poll do |executable|
        if executable.respond_to?(:chunk?) && executable.chunk?
          run_chunk(executable, reporter)
        else
          run_single_test(executable, reporter)
        end
      end
    end

    private

    def run_chunk(chunk, reporter)
      @in_chunk_context = true

      chunk_start_time = Time.now
      chunk_failed = false
      failed_tests = []

      puts "Running chunk: #{chunk.suite_name} (#{chunk.size} tests)" if ENV['VERBOSE']

      # Run each test in the chunk sequentially
      chunk.tests.each do |test|
        result = test.run
        test_failed = handle_test_result(test, result, reporter)

        if test_failed
          chunk_failed = true
          failed_tests << test
        end
      end

      # Acknowledge the chunk (not individual tests)
      queue.acknowledge(chunk)

      # Log chunk completion
      if ENV['VERBOSE']
        duration = Time.now - chunk_start_time
        status = chunk_failed ? "FAILED" : "PASSED"
        puts "Completed chunk: #{chunk.suite_name} (#{status}, #{duration.round(2)}s)"
      end

      # Requeue failed tests individually if needed
      requeue_failed_tests(failed_tests) if chunk_failed
    ensure
      @in_chunk_context = false
    end

    def run_single_test(test, reporter)
      result = test.run
      handle_test_result(test, result, reporter)
    end

    def handle_test_result(test, result, reporter)
      # Returns true if test failed
      failed = !(result.passed? || result.skipped?)

      if test.flaky?
        result.mark_as_flaked!
        failed = false
      end

      if failed
        queue.report_failure!
      else
        queue.report_success!
      end

      # Handle requeuing for single tests (not chunks)
      # Chunks handle requeuing separately
      requeued = false
      unless in_chunk_context?
        if failed && CI::Queue.requeueable?(result) && !queue.config.known_flaky?(test.id) && queue.requeue(test)
          requeued = true
          result.requeue!
          reporter.record(result)
        elsif queue.acknowledge(test) || !failed
          # If the test was already acknowledged by another worker (we timed out)
          # Then we only record it if it is successful.
          reporter.record(result)
        end

        if !requeued && failed
          queue.increment_test_failed
        end
      else
        # In chunk context - always record result
        reporter.record(result)

        if failed
          queue.increment_test_failed
        end
      end

      failed
    end

    def requeue_failed_tests(failed_tests)
      # Requeue failed tests individually (breaking them out of chunk)
      failed_tests.each do |test|
        if CI::Queue.requeueable?(test) && !queue.config.known_flaky?(test.id)
          queue.requeue(test, skip_reservation_check: true)
        end
      end
    end

    def in_chunk_context?
      # Track whether we're currently executing a chunk
      @in_chunk_context ||= false
    end
  end
end

MiniTest.singleton_class.prepend(MiniTest::Queue)
if defined? MiniTest::Result
  MiniTest::Result.prepend(MiniTest::Requeueing)
  MiniTest::Result.prepend(MiniTest::Flakiness)
  MiniTest::Result.prepend(MiniTest::WithTimestamps)
else
  MiniTest::Test.prepend(MiniTest::Requeueing)
  MiniTest::Test.prepend(MiniTest::Flakiness)
  MiniTest::Test.prepend(MiniTest::WithTimestamps)

  module MinitestBackwardCompatibility
    def source_location
      method(name).source_location
    end

    def klass
      self.class.name
    end
  end
  MiniTest::Test.prepend(MinitestBackwardCompatibility)
end
