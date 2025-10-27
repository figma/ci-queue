# frozen_string_literal: true
require 'test_helper'

class CI::Queue::RedisTest < Minitest::Test
  include SharedQueueAssertions

  def setup
    @redis_url = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
    @redis = ::Redis.new(url: @redis_url)
    @redis.flushdb
    super
    @config = @queue.send(:config) # hack
  end

  def test_from_uri
    second_queue = populate(
      CI::Queue.from_uri(@redis_url, config)
    )
    assert_instance_of CI::Queue::Redis::Worker, second_queue
    assert_equal @queue.to_a, second_queue.to_a
  end

  def test_requeue # redefine the shared one
    previous_offset = CI::Queue::Redis.requeue_offset
    CI::Queue::Redis.requeue_offset = 2
    failed_once = false
    test_order = poll(@queue, ->(test) {
      if test == shuffled_test_list.last && !failed_once
        failed_once = true
        false
      else
        true
      end
    })

    expected_order = shuffled_test_list.dup
    expected_order.insert(-CI::Queue::Redis.requeue_offset, shuffled_test_list.last)

    assert_equal expected_order, test_order
  ensure
    CI::Queue::Redis.requeue_offset = previous_offset
  end

  def test_retry_queue_with_all_tests_passing
    poll(@queue)
    retry_queue = @queue.retry_queue
    populate(retry_queue)
    retry_test_order = poll(retry_queue)
    assert_equal [], retry_test_order
  end

  def test_retry_queue_with_all_tests_passing_2
    poll(@queue)
    retry_queue = @queue.retry_queue
    populate(retry_queue)
    retry_test_order = poll(retry_queue) do |test|
      @queue.build.record_error(test.id, 'Failed')
    end
    assert_equal retry_test_order, retry_test_order
  end

  def test_shutdown
    poll(@queue) do
      @queue.shutdown!
    end
    assert_equal TEST_LIST.size - 1, @queue.size
  end

  def test_master_election
    assert_predicate @queue, :master?
    refute_predicate worker(2), :master?

    @redis.flushdb
    assert_predicate worker(2), :master?
    refute_predicate worker(1), :master?
  end

  def test_exhausted_while_not_populated
    assert_predicate @queue, :populated?

    second_worker = worker(2, populate: false)

    refute_predicate second_worker, :populated?
    refute_predicate second_worker, :exhausted?

    poll(@queue)

    refute_predicate second_worker, :populated?
    assert_predicate second_worker, :exhausted?
  end

  def test_timed_out_test_are_picked_up_by_other_workers
    second_queue = worker(2)
    acquired = false
    done = false
    monitor = Monitor.new
    condition = monitor.new_cond

    Thread.start do
      monitor.synchronize do
        condition.wait_until { acquired }
        poll(second_queue)
        done = true
        condition.signal
      end
    end

    poll(@queue) do
      acquired = true
      monitor.synchronize do
        condition.signal
        condition.wait_until { done }
      end
    end

    assert_predicate @queue, :exhausted?
    assert_equal [], populate(@queue.retry_queue).to_a
    assert_equal [], populate(second_queue.retry_queue).to_a.sort
  end

  def test_release_immediately_timeout_the_lease
    second_queue = worker(2)

    reserved_test = nil
    poll(@queue) do |test|
      reserved_test = test
      break
    end
    refute_nil reserved_test

    worker(1).release! # Use a new instance to ensure we don't depend on in-memory state

    poll(second_queue) do |test|
      assert_equal reserved_test, test
      break
    end
  end

  def test_test_isnt_requeued_if_it_was_picked_up_by_another_worker
    second_queue = worker(2)
    acquired = false
    done = false
    monitor = Monitor.new
    condition = monitor.new_cond

    Thread.start do
      monitor.synchronize do
        condition.wait_until { acquired }
        poll(second_queue)
        done = true
        condition.signal
      end
    end

    poll(@queue, false) do
      break if acquired
      acquired = true
      monitor.synchronize do
        condition.signal
        condition.wait_until { done }
      end
    end

    assert_predicate @queue, :exhausted?
  end

  def test_acknowledge_returns_false_if_the_test_was_picked_up_by_another_worker
    second_queue = worker(2)
    acquired = false
    done = false
    monitor = Monitor.new
    condition = monitor.new_cond

    Thread.start do
      monitor.synchronize do
        condition.wait_until { acquired }
        second_queue.poll do |test|
          assert_equal true, second_queue.acknowledge(test)
        end
        done = true
        condition.signal
      end
    end

    @queue.poll do |test|
      break if acquired
      acquired = true
      monitor.synchronize do
        condition.signal
        condition.wait_until { done }
        assert_equal false, @queue.acknowledge(test)
      end
    end

    assert_predicate @queue, :exhausted?
  end

  def test_workers_register
    assert_equal 1, @redis.scard(('build:42:workers'))
    worker(2)
    assert_equal 2, @redis.scard(('build:42:workers'))
  end

  def test_timeout_warning
    begin
      threads = 2.times.map do |i|
        Thread.new do
          queue = worker(i, tests: [TEST_LIST.first], build_id: '24')
          queue.poll do |test|
            sleep 1 # timeout
            queue.acknowledge(test)
          end
        end
      end

      threads.each { |t| t.join(3) }
      threads.each { |t| refute_predicate t, :alive? }

      queue = worker(12, build_id: '24')
      assert_equal [[:RESERVED_LOST_TEST, {test: 'ATest#test_foo', timeout: 0.2}]], queue.build.pop_warnings
    ensure
      threads.each(&:kill)
    end
  end

  def test_continuously_timing_out_tests
    3.times do
      @redis.flushdb
      begin
        threads = 2.times.map do |i|
          Thread.new do
            queue = worker(i, tests: [TEST_LIST.first], build_id: '24')
            queue.poll do |test|
              sleep 1 # timeout
              queue.acknowledge(test)
            end
          end
        end

        threads.each { |t| t.join(3) }
        threads.each { |t| refute_predicate t, :alive? }

        queue = worker(12, build_id: '24')
        assert_predicate queue, :queue_initialized?
        assert_predicate queue, :exhausted?
      ensure
        threads.each(&:kill)
      end
    end
  end

  def test_chunk_with_dynamic_timeout_not_stolen_by_other_worker
    # Test that chunks with dynamic timeout (timeout * test_count) are not
    # stolen by other workers before the dynamic timeout expires
    @redis.flushdb

    # Create a chunk with 10 tests from same suite -> timeout = 0.2s * 10 = 2.0s
    tests = (1..10).map { |i| MockTest.new("ChunkSuite#test_#{i}") }

    worker1 = worker(1, tests: tests, build_id: '100', strategy: :suite_bin_packing,
                     suite_max_duration: 120_000, timing_fallback_duration: 100.0)
    worker2 = worker(2, tests: tests, build_id: '100', strategy: :suite_bin_packing,
                     suite_max_duration: 120_000, timing_fallback_duration: 100.0)

    acquired = false
    worker2_tried = false
    worker2_got_test = false
    monitor = Monitor.new
    condition = monitor.new_cond

    # Worker 2 thread: waits for worker1 to acquire, then tries to steal (should fail)
    Thread.start do
      monitor.synchronize do
        condition.wait_until { acquired }
        # Wait less than dynamic timeout (0.5s < 2.0s)
        sleep 0.5
        # Try to poll once - should not get anything since chunk hasn't timed out
        # Use a timeout thread to force worker2 to give up after a short time
        timeout_thread = Thread.new do
          sleep 0.1 # Give worker2 a brief chance to try reserving
          worker2.shutdown!
        end
        worker2.poll do |test|
          worker2_got_test = true
          worker2.acknowledge(test)
          break
        end
        timeout_thread.kill
        worker2_tried = true
        condition.signal
      end
    end

    # Worker 1: acquires chunk and holds it
    reserved_test = nil
    worker1.poll do |test|
      reserved_test = test
      refute_nil reserved_test
      assert reserved_test.respond_to?(:chunk?) && reserved_test.chunk?, "Expected a chunk to be reserved"

      # Signal worker2 to try stealing, then wait for it to finish trying
      acquired = true
      monitor.synchronize do
        condition.signal
        condition.wait_until { worker2_tried }
      end

      # Now acknowledge the chunk
      worker1.acknowledge(test)
      break
    end

    refute worker2_got_test, "Worker 2 should not steal chunk before dynamic timeout expires"
  end

  def test_chunk_with_dynamic_timeout_picked_up_after_timeout
    @redis.flushdb

    tests = (1..5).map { |i| MockTest.new("TimeoutSuite#test_#{i}") }

    worker1 = worker(1, tests: tests, build_id: '101', strategy: :suite_bin_packing,
                     suite_max_duration: 120_000, timing_fallback_duration: 100.0)
    worker2 = worker(2, tests: tests, build_id: '101', strategy: :suite_bin_packing,
                     suite_max_duration: 120_000, timing_fallback_duration: 100.0)

    acquired = false
    done = false
    reserved_test = nil
    stolen_test = nil
    monitor = Monitor.new
    condition = monitor.new_cond

    # Worker 2 thread: waits for worker1 to acquire, then waits for timeout and steals
    Thread.start do
      monitor.synchronize do
        condition.wait_until { acquired }
        # Wait longer than dynamic timeout (1.2s > 1.0s)
        sleep 1.2
        # Now poll - should successfully steal the timed-out chunk
        worker2.poll do |test|
          stolen_test = test
          worker2.acknowledge(test)
          break
        end
        done = true
        condition.signal
      end
    end

    # Worker 1: acquires chunk and holds it without acknowledging
    worker1.poll do |test|
      reserved_test = test
      refute_nil reserved_test
      assert reserved_test.respond_to?(:chunk?) && reserved_test.chunk?, "Expected a chunk to be reserved"

      # Signal worker2 to start waiting, then wait for it to steal the chunk
      acquired = true
      monitor.synchronize do
        condition.signal
        condition.wait_until { done }
      end
      break
    end

    refute_nil stolen_test, "Worker 2 should pick up chunk after dynamic timeout expires"
    assert_equal reserved_test.id, stolen_test.id

    # Verify the RESERVED_LOST_TEST warning was recorded
    warnings = worker2.build.pop_warnings
    assert_equal 1, warnings.size
    assert_equal :RESERVED_LOST_TEST, warnings.first.first
  end

  def test_individual_test_uses_default_timeout_after_requeue
    # Test that individual tests (not in chunks) use the default timeout
    @redis.flushdb

    # Create individual tests from different suites (won't be chunked together)
    tests = [
      MockTest.new("SuiteA#test_1"),
      MockTest.new("SuiteB#test_1"),
      MockTest.new("SuiteC#test_1")
    ]

    worker1 = worker(1, tests: tests, build_id: '102', timeout: 0.2)
    worker2 = worker(2, tests: tests, build_id: '102', timeout: 0.2)

    acquired = false
    done = false
    reserved_test = nil
    stolen_test = nil
    monitor = Monitor.new
    condition = monitor.new_cond

    # Worker 2 thread: waits for worker1 to acquire, then waits for default timeout and steals
    Thread.start do
      monitor.synchronize do
        condition.wait_until { acquired }
        # Wait for default timeout (0.3s > 0.2s default)
        sleep 0.3
        # Now poll - should successfully steal the timed-out test
        worker2.poll do |test|
          stolen_test = test
          worker2.acknowledge(test)
          break
        end
        done = true
        condition.signal
      end
    end

    # Worker 1: acquires an individual test and holds it without acknowledging
    worker1.poll do |test|
      reserved_test = test
      refute_nil reserved_test
      refute (reserved_test.respond_to?(:chunk?) && reserved_test.chunk?), "Expected an individual test, not a chunk"

      # Signal worker2 to start waiting, then wait for it to steal the test
      acquired = true
      monitor.synchronize do
        condition.signal
        condition.wait_until { done }
      end
      break
    end

    refute_nil stolen_test, "Worker 2 should steal individual test after default timeout"
    assert_equal reserved_test.id, stolen_test.id
  end

  private

  class MockTest
    attr_reader :id

    def initialize(id)
      @id = id
    end

    def <=>(other)
      id <=> other.id
    end

    def flaky?
      false
    end

    def tests
      [self]
    end
  end

  def shuffled_test_list
    CI::Queue.shuffle(TEST_LIST, Random.new(0)).freeze
  end

  def build_queue
    worker(1, max_requeues: 1, requeue_tolerance: 0.1, populate: false, max_consecutive_failures: 10)
  end

  def populate(worker, tests: TEST_LIST.dup)
    worker.populate(tests, random: Random.new(0))
  end

  def worker(id, **args)
    tests = args.delete(:tests) || TEST_LIST.dup
    skip_populate = args.delete(:populate) == false
    queue = CI::Queue::Redis.new(
      @redis_url,
      CI::Queue::Configuration.new(
        build_id: '42',
        worker_id: id.to_s,
        timeout: 0.2,
        **args,
      )
    )
    if skip_populate
      return queue
    else
      populate(queue, tests: tests)
    end
  end
end
