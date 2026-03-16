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

  def teardown
    @redis.flushdb
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
    test_order = poll(@queue, lambda { |test|
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
    assert_equal 1, @redis.scard('build:42:workers')
    worker(2)
    assert_equal 2, @redis.scard('build:42:workers')
  end

  def test_timeout_warning
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
    assert_equal [[:RESERVED_LOST_TEST, { test: 'ATest#test_foo', timeout: 0.2 }]], queue.build.pop_warnings
  ensure
    threads.each(&:kill)
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
                        suite_max_duration: 120_000, timing_fallback_duration: 100.0, populate: false)

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
      assert reserved_test.respond_to?(:chunk?) && reserved_test.chunk?, 'Expected a chunk to be reserved'

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

    refute worker2_got_test, 'Worker 2 should not steal chunk before dynamic timeout expires'
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
      assert reserved_test.respond_to?(:chunk?) && reserved_test.chunk?, 'Expected a chunk to be reserved'

      # Signal worker2 to start waiting, then wait for it to steal the chunk
      acquired = true
      monitor.synchronize do
        condition.signal
        condition.wait_until { done }
      end
      break
    end

    refute_nil stolen_test, 'Worker 2 should pick up chunk after dynamic timeout expires'
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
      MockTest.new('SuiteA#test_1'),
      MockTest.new('SuiteB#test_1'),
      MockTest.new('SuiteC#test_1')
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
      refute (reserved_test.respond_to?(:chunk?) && reserved_test.chunk?), 'Expected an individual test, not a chunk'

      # Signal worker2 to start waiting, then wait for it to steal the test
      acquired = true
      monitor.synchronize do
        condition.signal
        condition.wait_until { done }
      end
      break
    end

    refute_nil stolen_test, 'Worker 2 should steal individual test after default timeout'
    assert_equal reserved_test.id, stolen_test.id
  end

  def test_suite_bin_packing_uses_moving_average_for_duration
    @redis.flushdb

    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis)
    updater.update_batch([['TestSuite#test_1', 5000.0], ['TestSuite#test_2', 3000.0]])

    tests = [
      MockTest.new('TestSuite#test_1'),
      MockTest.new('TestSuite#test_2')
    ]

    worker = worker(1, tests: tests, build_id: '200', strategy: :suite_bin_packing,
                       suite_max_duration: 120_000, timing_fallback_duration: 100.0)

    chunks = []
    worker.poll do |chunk|
      chunks << chunk
      worker.acknowledge(chunk)
    end

    assert_equal 1, chunks.size
    chunk = chunks.first
    assert chunk.chunk?, 'Expected a chunk'
    assert_equal 'TestSuite:chunk_0', chunk.id
    assert_equal 8000.0, chunk.estimated_duration
  end

  def test_moving_average_takes_precedence_over_timing_file
    @redis.flushdb
    require 'tempfile'

    timing_data = { 'TestSuite#test_1' => 10_000.0 }

    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis)
    updater.update_batch([['TestSuite#test_1', 2000.0]])

    tests = [MockTest.new('TestSuite#test_1')]

    Tempfile.open(['timing', '.json']) do |file|
      file.write(JSON.generate(timing_data))
      file.close

      worker = worker(1, tests: tests, build_id: '201', strategy: :suite_bin_packing,
                         suite_max_duration: 120_000, timing_fallback_duration: 100.0,
                         timing_file: file.path)

      chunks = []
      worker.poll do |chunk|
        chunks << chunk
        worker.acknowledge(chunk)
      end

      assert_equal 1, chunks.size
      assert_equal 2000.0, chunks.first.estimated_duration
    end
  end

  def test_falls_back_to_timing_file_when_no_moving_average
    @redis.flushdb
    require 'tempfile'

    timing_data = { 'TestSuite#test_1' => 7000.0 }
    tests = [MockTest.new('TestSuite#test_1')]

    Tempfile.open(['timing', '.json']) do |file|
      file.write(JSON.generate(timing_data))
      file.close

      worker = worker(1, tests: tests, build_id: '202', strategy: :suite_bin_packing,
                         suite_max_duration: 120_000, timing_fallback_duration: 100.0,
                         timing_file: file.path)

      chunks = []
      worker.poll do |chunk|
        chunks << chunk
        worker.acknowledge(chunk)
      end

      assert_equal 1, chunks.size
      assert_equal 7000.0, chunks.first.estimated_duration
    end
  end

  def test_falls_back_to_default_when_no_moving_average_or_timing_data
    @redis.flushdb

    tests = [MockTest.new('UnknownTest#test_1')]

    worker = worker(1, tests: tests, build_id: '203', strategy: :suite_bin_packing,
                       suite_max_duration: 120_000, timing_fallback_duration: 500.0)

    chunks = []
    worker.poll do |chunk|
      chunks << chunk
      worker.acknowledge(chunk)
    end

    assert_equal 1, chunks.size
    assert_equal 500.0, chunks.first.estimated_duration
  end

  def test_mixed_duration_sources_in_suite_splitting
    @redis.flushdb
    require 'tempfile'

    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis)
    updater.update_batch([['MixedTest#test_1', 60_000.0], ['MixedTest#test_2', 50_000.0]])

    timing_data = {
      'MixedTest#test_3' => 40_000.0,
      'MixedTest#test_4' => 30_000.0
    }

    tests = [
      MockTest.new('MixedTest#test_1'),
      MockTest.new('MixedTest#test_2'),
      MockTest.new('MixedTest#test_3'),
      MockTest.new('MixedTest#test_4')
    ]

    Tempfile.open(['timing', '.json']) do |file|
      file.write(JSON.generate(timing_data))
      file.close

      worker = worker(1, tests: tests, build_id: '204', strategy: :suite_bin_packing,
                         suite_max_duration: 120_000, suite_buffer_percent: 10,
                         timing_fallback_duration: 100.0, timing_file: file.path)

      chunks = []
      worker.poll do |chunk|
        chunks << chunk
        worker.acknowledge(chunk)
      end

      assert chunks.size >= 2

      effective_max = 120_000 * (1 - 10 / 100.0)
      chunks.each do |chunk|
        assert chunk.estimated_duration <= effective_max,
               "Chunk duration #{chunk.estimated_duration} exceeds effective max #{effective_max}"
      end
    end
  end

  def test_moving_average_ordering_by_duration
    @redis.flushdb

    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis)
    updater.update_batch([['FastTest#test_1', 1000.0], ['SlowTest#test_1', 10_000.0], ['MediumTest#test_1', 5000.0]])

    tests = [
      MockTest.new('FastTest#test_1'),
      MockTest.new('SlowTest#test_1'),
      MockTest.new('MediumTest#test_1')
    ]

    worker = worker(1, tests: tests, build_id: '205', strategy: :suite_bin_packing,
                       suite_max_duration: 120_000, timing_fallback_duration: 100.0)

    chunks = []
    worker.poll do |chunk|
      chunks << chunk
      worker.acknowledge(chunk)
    end

    # Should be ordered by duration descending: SlowTest, MediumTest, FastTest
    assert_equal 3, chunks.size
    assert_equal 'SlowTest:chunk_0', chunks[0].id
    assert_equal 10_000.0, chunks[0].estimated_duration
    assert_equal 'MediumTest:chunk_0', chunks[1].id
    assert_equal 5000.0, chunks[1].estimated_duration
    assert_equal 'FastTest:chunk_0', chunks[2].id
    assert_equal 1000.0, chunks[2].estimated_duration
  end

  def test_moving_average_with_partial_coverage
    @redis.flushdb

    # Only one test has moving average data
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis)
    updater.update_batch([['PartialTest#test_1', 3000.0]])

    tests = [
      MockTest.new('PartialTest#test_1'),
      MockTest.new('PartialTest#test_2'),
      MockTest.new('PartialTest#test_3')
    ]

    worker = worker(1, tests: tests, build_id: '206', strategy: :suite_bin_packing,
                       suite_max_duration: 120_000, timing_fallback_duration: 500.0)

    chunks = []
    worker.poll do |chunk|
      chunks << chunk
      worker.acknowledge(chunk)
    end

    assert_equal 1, chunks.size
    assert_equal 4000.0, chunks.first.estimated_duration
  end

  def test_moving_average_updates_persist_across_workers
    @redis.flushdb

    # Manually update moving average as if a previous worker completed the test
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis)
    updater.update_batch([['PersistTest#test_1', 5500.0]])

    # New worker should see the persisted moving average
    tests = [MockTest.new('PersistTest#test_1')]
    worker1 = worker(1, tests: tests, build_id: '207', strategy: :suite_bin_packing,
                        suite_max_duration: 120_000, timing_fallback_duration: 1000.0)

    chunks = []
    worker1.poll do |chunk|
      chunks << chunk
      worker1.acknowledge(chunk)
    end

    # Should use the persisted moving average value
    assert_equal 1, chunks.size
    assert_equal 5500.0, chunks.first.estimated_duration
  end

  # --- Election recovery tests ---

  def test_master_death_triggers_re_election
    build = 'election-death'
    w1 = worker(1, build_id: build, master_lock_ttl: 1, max_election_attempts: 3)
    assert_predicate w1, :master?

    # Simulate master death: delete the master-status key (lock expires)
    @redis.del("build:#{build}:master-status")

    # Worker 2 should detect the dead master and become the new master
    w2 = worker(2, build_id: build, master_lock_ttl: 1, max_election_attempts: 3)
    assert_predicate w2, :master?

    # Both workers should be able to poll successfully
    poll(w2)
    assert_predicate w2, :exhausted?
  end

  def test_fenced_push_rejects_stale_master
    build = 'fenced-push'
    w1 = worker(1, build_id: build, master_lock_ttl: 30)
    assert_predicate w1, :master?

    # Overwrite master-status to simulate another master winning election
    other_gen = 'other-generation-uuid'
    @redis.set("build:#{build}:master-status", "setup:#{other_gen}", ex: 30)

    # w1 still thinks it's master, but push.lua should reject because lock value changed
    w1.send(:push, ['ATest#test_foo'])

    # master-status should still be the other master's value
    status = @redis.get("build:#{build}:master-status")
    assert_equal "setup:#{other_gen}", status, "Fenced push should not overwrite another master's status"
  end

  def test_generation_stale_exits_poll
    build = 'gen-stale'
    w1 = worker(1, build_id: build)
    assert_predicate w1, :master?

    w2 = worker(2, build_id: build)
    refute_predicate w2, :master?

    # Drain the queue with w1 so w2 sees no tests (idle), but not exhausted
    poll(w1)

    # Change generation before w2 polls — w2 will be idle and detect staleness
    @redis.set("build:#{build}:current-generation", "new-generation-uuid")

    tests_seen = []
    w2.poll do |test|
      tests_seen << test
      w2.acknowledge(test)
    end

    assert_equal 0, tests_seen.size, "Worker should exit poll immediately when generation is stale"
  end

  def test_learn_generation_raises_when_key_missing
    build = 'learn-gen-missing'
    w1 = worker(1, build_id: build)
    assert_predicate w1, :master?

    # Delete the current-generation key
    @redis.del("build:#{build}:current-generation")

    # A non-master worker trying to learn generation should raise MasterDied
    assert_raises(CI::Queue::Redis::MasterDied) do
      w2 = worker(2, build_id: build, populate: false)
      w2.send(:learn_generation)
    end
  end

  def test_max_election_attempts_raises_lost_master
    build = 'max-attempts'

    # Worker 1 wins master and starts setup, then dies (lock expires)
    # We simulate this by having another process hold setup then expire
    # To prevent the test worker from winning master, keep re-setting the key
    # so it always sees "setup" then nil (death)
    t = Thread.new do
      loop do
        # Keep setting a short-lived setup lock so the worker always sees a dying master
        @redis.set("build:#{build}:master-status", "setup:dead-gen", px: 50)
        sleep 0.06
      end
    end

    assert_raises(CI::Queue::Redis::LostMaster) do
      worker(1, build_id: build, max_election_attempts: 1, queue_init_timeout: 0.5)
    end
  ensure
    t&.kill
  end

  def test_build_record_reads_generation_scoped_requeue_key
    build = 'requeue-gen'
    w1 = worker(1, build_id: build, max_requeues: 1, requeue_tolerance: 1.0)

    w1.poll do |test|
      w1.report_failure!
      unless w1.requeue(test)
        w1.acknowledge(test)
      end
    end

    requeues = w1.build.requeued_tests
    refute_empty requeues, "Should have requeued at least one test"
  end

  def test_supervisor_handles_master_died
    build = 'supervisor-died'
    supervisor = CI::Queue::Redis::Supervisor.new(
      @redis_url,
      CI::Queue::Configuration.new(
        build_id: build,
        worker_id: 'sup',
        timeout: 0.2,
        queue_init_timeout: 0.3,
        timing_redis_url: @redis_url,
      )
    )

    # wait_for_workers should return false (not crash) when no master exists
    result = supervisor.wait_for_workers
    refute result, "Supervisor should return false when master never appeared"
  end

  def test_wait_for_master_detects_immediate_nil_status
    build = 'nil-status'

    w = CI::Queue::Redis.new(
      @redis_url,
      CI::Queue::Configuration.new(
        build_id: build,
        worker_id: '2',
        timeout: 0.2,
        queue_init_timeout: 0.5,
        timing_redis_url: @redis_url,
      )
    )

    # Simulate that status was "setup" then expired (nil)
    @redis.set("build:#{build}:master-status", "setup:some-gen", px: 1)
    sleep 0.01 # Let it expire

    assert_raises(CI::Queue::Redis::MasterDied) do
      w.send(:wait_for_master, timeout: 0.5)
    end
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

  def test_heartbeat_returns_true_for_owned_test
    reserved_test = nil
    @queue.poll do |test|
      reserved_test = test
      break
    end
    refute_nil reserved_test

    # Heartbeat should succeed for currently reserved test
    assert @queue.heartbeat(reserved_test), 'Heartbeat should succeed for owned test'
  end

  def test_heartbeat_returns_false_for_unowned_test
    second_queue = worker(2)
    reserved_test = nil

    @queue.poll do |test|
      reserved_test = test
      break
    end
    refute_nil reserved_test

    # Second queue should not be able to heartbeat a test it doesn't own
    refute second_queue.heartbeat(reserved_test), 'Heartbeat should fail for unowned test'
  end

  def test_heartbeat_returns_false_for_processed_test
    reserved_test = nil
    @queue.poll do |test|
      reserved_test = test
      @queue.acknowledge(test)
      break
    end
    refute_nil reserved_test

    # Heartbeat should fail for already processed test
    # Need a new worker instance since the original one no longer has the test reserved
    new_queue = worker(1)
    refute new_queue.heartbeat(reserved_test), 'Heartbeat should fail for processed test'
  end

  def test_heartbeat_extends_deadline_and_prevents_stealing
    # Use a short timeout and short grace period for testing
    first_queue = worker(1, tests: [TEST_LIST.first], build_id: '100', timeout: 0.5, heartbeat_grace_period: 0.3)
    second_queue = worker(2, tests: [TEST_LIST.first], build_id: '100', timeout: 0.5, heartbeat_grace_period: 0.3)

    reserved_test = nil
    stolen = false

    # First worker reserves the test
    first_queue.poll do |test|
      reserved_test = test
      # Wait for deadline to pass
      sleep 0.6

      # Before the deadline passes, send a heartbeat to extend it
      assert first_queue.heartbeat(test), 'Heartbeat should succeed'

      # Now try to have second worker steal - should fail because of recent heartbeat
      second_queue.poll do |stolen_test|
        stolen = true
        second_queue.acknowledge(stolen_test)
      end

      first_queue.acknowledge(test)
    end

    refute_nil reserved_test
    refute stolen, 'Second worker should not steal test with recent heartbeat'
  end

  def test_stale_heartbeat_allows_stealing
    # Use a short timeout and very short grace period for testing
    first_queue = worker(1, tests: [TEST_LIST.first], build_id: '101', timeout: 0.3, heartbeat_grace_period: 0.2)
    second_queue = worker(2, tests: [TEST_LIST.first], build_id: '101', timeout: 0.3, heartbeat_grace_period: 0.2)

    reserved_test = nil
    stolen_test = nil

    # First worker reserves the test
    first_queue.poll do |test|
      reserved_test = test

      # Send a heartbeat
      assert first_queue.heartbeat(test), 'Heartbeat should succeed'

      # Wait for both the heartbeat grace period AND the extended deadline to pass
      sleep 0.6

      # Now second worker should be able to steal since heartbeat is stale
      second_queue.poll do |test2|
        stolen_test = test2
        second_queue.acknowledge(test2)
      end
    end

    refute_nil reserved_test
    refute_nil stolen_test, 'Second worker should steal test with stale heartbeat'
    assert_equal reserved_test.id, stolen_test.id
  end

  def shuffled_test_list
    TEST_LIST.sort.shuffle(random: Random.new(0)).freeze
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
        timing_redis_url: @redis_url,
        **args
      )
    )
    return queue if skip_populate

    populate(queue, tests: tests)
  end
end
