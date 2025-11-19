# frozen_string_literal: true
require 'test_helper'
require 'set'

class QueueChunkTest < Minitest::Test
  def setup
    @queue_impl = MockQueue.new
    @reporter = MockReporter.new
  end

  def test_run_from_queue_handles_single_test
    test = MockTest.new('TestA#test_1')
    result = MockResult.new(passed: true)

    test.stub(:run, result) do
      @queue_impl.add_test(test)

      runner = create_runner(@queue_impl)
      runner.run_from_queue(@reporter)
    end

    assert_includes @reporter.recorded, result
    assert_equal 1, @queue_impl.acknowledged.size
    assert_includes @queue_impl.acknowledged, 'TestA#test_1'
  end

  def test_run_from_queue_handles_chunk
    test1 = MockTest.new('TestA#test_1')
    test2 = MockTest.new('TestA#test_2')
    result1 = MockResult.new(passed: true)
    result2 = MockResult.new(passed: true)

    chunk = MockResolvedChunk.new('TestA:chunk_0', 'TestA', [test1, test2])

    test1.stub(:run, result1) do
      test2.stub(:run, result2) do
        @queue_impl.add_test(chunk)

        runner = create_runner(@queue_impl)
        runner.run_from_queue(@reporter)
      end
    end

    # Both test results should be recorded
    assert_includes @reporter.recorded, result1
    assert_includes @reporter.recorded, result2

    # Chunk should be acknowledged (by its ID)
    assert_equal 1, @queue_impl.acknowledged.size
    assert_includes @queue_impl.acknowledged, 'TestA:chunk_0'
  end

  def test_chunk_execution_continues_after_failure
    test1 = MockTest.new('TestA#test_1')
    test2 = MockTest.new('TestA#test_2')
    result1 = MockResult.new(passed: false)
    result2 = MockResult.new(passed: true)

    chunk = MockResolvedChunk.new('TestA:chunk_0', 'TestA', [test1, test2])

    test1.stub(:run, result1) do
      test2.stub(:run, result2) do
        @queue_impl.add_test(chunk)

        runner = create_runner(@queue_impl)
        runner.run_from_queue(@reporter)
      end
    end

    # Both results should be recorded even though first failed
    assert_equal 2, @reporter.recorded.size
    assert_includes @reporter.recorded, result1
    assert_includes @reporter.recorded, result2
  end

  def test_duck_typing_for_chunk_detection
    chunk = MockResolvedChunk.new('TestA:chunk_0', 'TestA', [])

    assert chunk.respond_to?(:chunk?)
    assert chunk.chunk?
  end

  def test_failed_test_in_chunk_is_requeued_when_not_known_flaky
    test1 = MockTest.new('TestA#test_1')
    test2 = MockTest.new('TestA#test_2')
    result1 = MockResult.new(passed: false)
    result2 = MockResult.new(passed: true)

    chunk = MockResolvedChunk.new('TestA:chunk_0', 'TestA', [test1, test2])

    # Stub CI::Queue.requeueable? to return true for failed tests
    CI::Queue.stub(:requeueable?, true) do
      test1.stub(:run, result1) do
        test2.stub(:run, result2) do
          @queue_impl.add_test(chunk)

          runner = create_runner(@queue_impl)
          runner.run_from_queue(@reporter)
        end
      end
    end

    # Both results should be recorded
    assert_equal 2, @reporter.recorded.size
    assert_includes @reporter.recorded, result1
    assert_includes @reporter.recorded, result2

    # Chunk should be acknowledged
    assert_equal 1, @queue_impl.acknowledged.size
    assert_includes @queue_impl.acknowledged, 'TestA:chunk_0'

    # Failed test should be requeued individually
    assert_equal 1, @queue_impl.requeued_tests.size
    assert_equal test1, @queue_impl.requeued_tests.first
  end

  def test_known_flaky_test_in_chunk_is_not_requeued
    test1 = MockTest.new('TestA#test_1')
    test2 = MockTest.new('TestA#test_2')
    result1 = MockResult.new(passed: false)
    result2 = MockResult.new(passed: true)

    # Configure test1 as a known flaky test
    @queue_impl.config = CI::Queue::Configuration.new(
      known_flaky_tests: Set.new(['TestA#test_1'])
    )

    chunk = MockResolvedChunk.new('TestA:chunk_0', 'TestA', [test1, test2])

    CI::Queue.stub(:requeueable?, true) do
      test1.stub(:run, result1) do
        test2.stub(:run, result2) do
          @queue_impl.add_test(chunk)

          runner = create_runner(@queue_impl)
          runner.run_from_queue(@reporter)
        end
      end
    end

    # Chunk should be acknowledged
    assert_equal 1, @queue_impl.acknowledged.size
    assert_includes @queue_impl.acknowledged, 'TestA:chunk_0'

    # Known flaky test should NOT be requeued even though it failed
    assert_equal 0, @queue_impl.requeued_tests.size
  end

  private

  def create_runner(queue_impl)
    runner = Object.new
    runner.define_singleton_method(:queue) { queue_impl }
    runner.extend(Minitest::Queue)
    runner
  end

  class MockQueue
    attr_reader :acknowledged, :requeued_tests
    attr_reader :success_count, :failure_count
    attr_accessor :config

    def initialize
      @tests = []
      @acknowledged = []
      @requeued_tests = []
      @success_count = 0
      @failure_count = 0
      @config = CI::Queue::Configuration.new
    end

    def add_test(test)
      @tests << test
    end

    def poll
      while (test = @tests.shift)
        yield test if block_given?
      end
    end

    def acknowledge(test_or_id)
      # Accept either an object with .id or a string, store the ID
      id = test_or_id.respond_to?(:id) ? test_or_id.id : test_or_id
      @acknowledged << id
    end

    def requeue(test, **kwargs)
      @requeued_tests << test
      false # Don't actually requeue in test
    end

    def report_success!
      @success_count += 1
    end

    def report_failure!
      @failure_count += 1
    end

    def increment_test_failed
      # No-op for testing
    end
  end

  class MockReporter
    attr_reader :recorded

    def initialize
      @recorded = []
    end

    def record(result)
      @recorded << result
    end
  end

  class MockTest
    attr_reader :id

    def initialize(id)
      @id = id
    end

    def run
      # Stubbed in tests
    end

    def flaky?
      false
    end
  end

  class MockResult
    attr_reader :passed

    def initialize(passed:)
      @passed = passed
    end

    def passed?
      @passed
    end

    def skipped?
      false
    end

    def mark_as_flaked!
      # No-op
    end

    def requeue!
      # No-op
    end
  end

  class MockResolvedChunk
    attr_reader :chunk_id, :suite_name, :tests

    def initialize(chunk_id, suite_name, tests)
      @chunk_id = chunk_id
      @suite_name = suite_name
      @tests = tests
    end

    def id
      chunk_id
    end

    def chunk?
      true
    end

    def size
      tests.size
    end

    def flaky?
      tests.any?(&:flaky?)
    end
  end
end
