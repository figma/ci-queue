# frozen_string_literal: true

require 'test_helper'
require 'timeout'

module Integration
  class MasterSetupHeartbeatIntegrationTest < Minitest::Test
    TEST_LIST = %w[
      ATest#test_foo
      ATest#test_bar
      BTest#test_foo
      BTest#test_bar
      CTest#test_foo
    ].freeze

    def setup
      @redis_url = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
      @redis = ::Redis.new(url: @redis_url)
      @redis.flushdb
      @threads = []
    end

    def teardown
      @threads.each(&:kill)
      @threads.each { |t| t.join(1) }
      @redis.flushdb
    end

    # === Scenario: Master dies during setup, follower takes over ===

    def test_follower_takes_over_when_master_dies_during_setup
      # Simulate master that starts setup but dies before completing
      # by setting up the 'setup' state without completing it

      # Create a "dead" master state
      @redis.set('build:100:master-status', 'setup')
      @redis.set('build:100:master-worker-id', 'dead_master')
      # Set a stale heartbeat (old enough to trigger takeover)
      old_time = CI::Queue.time_now.to_f - 60
      @redis.set('build:100:master-setup-heartbeat', old_time)

      # Start a follower worker that should detect dead master and take over
      tests = TEST_LIST.map { |id| MockTest.new(id) }
      follower = create_worker(
        1,
        build_id: '100',
        master_setup_heartbeat_timeout: 30,
        master_setup_heartbeat_interval: 0.2
      )

      # Populate will trigger wait_for_master which should detect dead master
      follower.populate(tests, random: Random.new(0))

      # Follower should have taken over
      assert follower.master?, 'Follower should become master after takeover'
      assert_equal '1', @redis.get('build:100:master-worker-id')
      assert_equal 'ready', @redis.get('build:100:master-status')
      assert_equal TEST_LIST.size, @redis.llen('build:100:queue')
    end

    def test_multiple_followers_competing_for_takeover
      # Simulate dead master
      @redis.set('build:101:master-status', 'setup')
      @redis.set('build:101:master-worker-id', 'dead_master')
      old_time = CI::Queue.time_now.to_f - 60
      @redis.set('build:101:master-setup-heartbeat', old_time)

      tests = TEST_LIST.map { |id| MockTest.new(id) }
      workers = []
      results = []
      mutex = Mutex.new

      # Start multiple followers simultaneously
      5.times do |i|
        @threads << Thread.new do
          worker = create_worker(
            i + 1,
            build_id: '101',
            master_setup_heartbeat_timeout: 30,
            master_setup_heartbeat_interval: 0.1
          )
          begin
            worker.populate(tests.map { |t| MockTest.new(t.id) }, random: Random.new(0))
            mutex.synchronize do
              workers << worker
              results << worker.master?
            end
          rescue StandardError => e
            mutex.synchronize { results << false }
          end
        end
      end

      # Wait for all threads to complete
      @threads.each { |t| t.join(10) }

      # Exactly one worker should be master
      master_count = results.count(true)
      assert_equal 1, master_count, "Expected exactly 1 master, got #{master_count}"

      # Queue should be properly set up
      assert_equal 'ready', @redis.get('build:101:master-status')
      assert_equal TEST_LIST.size, @redis.llen('build:101:queue')
    end

    def test_follower_waits_for_healthy_master_to_complete
      tests = TEST_LIST.map { |id| MockTest.new(id) }
      master_completed = false
      follower_completed = false

      # Start master worker that takes time to complete setup
      @threads << Thread.new do
        master = create_worker(
          1,
          build_id: '102',
          master_setup_heartbeat_interval: 0.1
        )
        # Simulate slow setup by adding sleep in the middle
        # The with_master_setup_heartbeat should keep heartbeat fresh
        master.populate(tests.map { |t| MockTest.new(t.id) }, random: Random.new(0))
        master_completed = true
      end

      # Give master time to start setup
      sleep 0.2

      # Start follower - should wait for master, not take over
      @threads << Thread.new do
        follower = create_worker(
          2,
          build_id: '102',
          master_setup_heartbeat_timeout: 30,
          master_setup_heartbeat_interval: 0.1
        )
        follower.populate(tests.map { |t| MockTest.new(t.id) }, random: Random.new(0))
        follower_completed = true
        refute follower.master?, 'Follower should not become master when master is healthy'
      end

      # Wait for both to complete
      @threads.each { |t| t.join(10) }

      assert master_completed, 'Master should have completed'
      assert follower_completed, 'Follower should have completed'

      # Master should still be worker 1
      assert_equal '1', @redis.get('build:102:master-worker-id')
    end

    def test_heartbeat_prevents_premature_takeover
      # Test that a fresh heartbeat prevents takeover even with short timeout

      # First, a real master starts up and begins setup
      tests = TEST_LIST.map { |id| MockTest.new(id) }

      # Create master and acquire role
      master = create_worker(
        1,
        build_id: '103',
        master_setup_heartbeat_interval: 0.1
      )

      # Manually acquire master role to control timing
      master.instance_variable_set(:@index, tests.map { |t| [t.id, t] }.to_h)
      master.instance_variable_set(:@total, tests.size)
      master.instance_variable_set(:@tests, tests)
      master.instance_variable_set(:@random, Random.new(0))

      # Acquire master role - this sets up initial state
      master.send(:acquire_master_role?)
      assert master.master?, 'Worker 1 should be master'

      # Send a fresh heartbeat
      master.send(:send_master_setup_heartbeat)

      # Now try to have another worker take over - should fail
      worker2 = create_worker(
        2,
        build_id: '103',
        master_setup_heartbeat_timeout: 30,
        master_setup_heartbeat_interval: 0.1
      )

      # Worker 2 should not be able to take over because heartbeat is fresh
      refute worker2.send(:attempt_master_takeover), 'Should not takeover when heartbeat is fresh'
      assert_equal '1', @redis.get('build:103:master-worker-id')
    end

    def test_end_to_end_normal_operation_with_heartbeat
      # Test that normal operation with heartbeat works correctly
      tests = TEST_LIST.map { |id| MockTest.new(id) }
      processed_tests = []
      mutex = Mutex.new

      # Start master
      master = create_worker(
        1,
        build_id: '104',
        master_setup_heartbeat_interval: 0.2
      )
      master.populate(tests.map { |t| MockTest.new(t.id) }, random: Random.new(0))

      # Start follower
      follower = create_worker(
        2,
        build_id: '104',
        master_setup_heartbeat_interval: 0.2
      )
      follower.populate(tests.map { |t| MockTest.new(t.id) }, random: Random.new(0))

      # Process tests from both workers
      [master, follower].each do |worker|
        @threads << Thread.new do
          worker.poll do |test|
            mutex.synchronize { processed_tests << test.id }
            worker.acknowledge(test)
          end
        end
      end

      # Wait for processing to complete
      @threads.each { |t| t.join(10) }

      # All tests should be processed
      assert_equal TEST_LIST.sort, processed_tests.sort
      assert master.exhausted?
      assert follower.exhausted?
    end

    def test_takeover_with_immediate_queue_initialization
      # Test that after takeover, the new master properly initializes the queue
      tests = TEST_LIST.map { |id| MockTest.new(id) }

      # Set up dead master state
      @redis.set('build:105:master-status', 'setup')
      @redis.set('build:105:master-worker-id', 'dead_master')
      old_time = CI::Queue.time_now.to_f - 60
      @redis.set('build:105:master-setup-heartbeat', old_time)

      # New worker takes over
      new_master = create_worker(
        1,
        build_id: '105',
        master_setup_heartbeat_timeout: 30,
        master_setup_heartbeat_interval: 0.1
      )
      new_master.populate(tests.map { |t| MockTest.new(t.id) }, random: Random.new(0))

      # Verify queue is fully functional
      assert new_master.master?
      assert new_master.queue_initialized?
      refute new_master.exhausted?
      assert_equal TEST_LIST.size, new_master.size
    end

    def test_no_takeover_when_master_finishes_quickly
      # Test that no takeover happens when master finishes before timeout
      tests = TEST_LIST.map { |id| MockTest.new(id) }

      # Master completes setup quickly
      master = create_worker(
        1,
        build_id: '106',
        master_setup_heartbeat_interval: 0.1
      )
      master.populate(tests.map { |t| MockTest.new(t.id) }, random: Random.new(0))

      # Give it a moment
      sleep 0.1

      # Follower joins - should not attempt takeover
      follower = create_worker(
        2,
        build_id: '106',
        master_setup_heartbeat_timeout: 30,
        master_setup_heartbeat_interval: 0.1
      )
      follower.populate(tests.map { |t| MockTest.new(t.id) }, random: Random.new(0))

      # Master should still be worker 1
      assert master.master?
      refute follower.master?
      assert_equal '1', @redis.get('build:106:master-worker-id')
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

    def create_worker(id, **options)
      build_id = options.delete(:build_id) || '42'
      config_options = {
        build_id: build_id,
        worker_id: id.to_s,
        timeout: 1,
        timing_redis_url: @redis_url,
        master_setup_heartbeat_interval: options.delete(:master_setup_heartbeat_interval) || 5,
        master_setup_heartbeat_timeout: options.delete(:master_setup_heartbeat_timeout) || 30
      }.merge(options)

      CI::Queue::Redis.new(
        @redis_url,
        CI::Queue::Configuration.new(**config_options)
      )
    end
  end
end
