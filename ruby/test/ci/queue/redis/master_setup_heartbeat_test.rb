# frozen_string_literal: true

require 'test_helper'

module CI
  module Queue
    module Redis
      class MasterSetupHeartbeatTest < Minitest::Test
        TEST_LIST = %w[
          ATest#test_foo
          ATest#test_bar
          BTest#test_foo
        ].freeze

        def setup
          @redis_url = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
          @redis = ::Redis.new(url: @redis_url)
          @redis.flushdb
        end

        def teardown
          @redis.flushdb
        end

        # === Configuration Tests ===

        def test_master_setup_heartbeat_interval_default
          config = Configuration.new
          assert_equal 5, config.master_setup_heartbeat_interval
        end

        def test_master_setup_heartbeat_timeout_default
          config = Configuration.new
          assert_equal 30, config.master_setup_heartbeat_timeout
        end

        def test_master_setup_heartbeat_interval_configurable
          config = Configuration.new(master_setup_heartbeat_interval: 10)
          assert_equal 10, config.master_setup_heartbeat_interval
        end

        def test_master_setup_heartbeat_timeout_configurable
          config = Configuration.new(master_setup_heartbeat_timeout: 60)
          assert_equal 60, config.master_setup_heartbeat_timeout
        end

        # === master_setup_heartbeat_stale? Tests ===

        def test_master_setup_heartbeat_stale_when_no_heartbeat_exists
          worker = create_worker(1)
          assert worker.master_setup_heartbeat_stale?, 'Should be stale when no heartbeat exists'
        end

        def test_master_setup_heartbeat_not_stale_when_fresh
          # Set a fresh heartbeat
          @redis.set('build:42:master-setup-heartbeat', CI::Queue.time_now.to_f)

          worker = create_worker(1, master_setup_heartbeat_timeout: 30)
          refute worker.master_setup_heartbeat_stale?, 'Should not be stale when heartbeat is fresh'
        end

        def test_master_setup_heartbeat_stale_when_old
          # Set a stale heartbeat (older than timeout)
          old_time = CI::Queue.time_now.to_f - 60 # 60 seconds ago
          @redis.set('build:42:master-setup-heartbeat', old_time)

          worker = create_worker(1, master_setup_heartbeat_timeout: 30)
          assert worker.master_setup_heartbeat_stale?, 'Should be stale when heartbeat is older than timeout'
        end

        def test_master_setup_heartbeat_exactly_at_timeout
          # Set heartbeat exactly at timeout boundary
          timeout = 30
          boundary_time = CI::Queue.time_now.to_f - timeout
          @redis.set('build:42:master-setup-heartbeat', boundary_time)

          worker = create_worker(1, master_setup_heartbeat_timeout: timeout)
          assert worker.master_setup_heartbeat_stale?, 'Should be stale at exactly timeout'
        end

        # === send_master_setup_heartbeat Tests ===

        def test_send_master_setup_heartbeat_sets_key
          worker = create_worker(1)
          worker.send(:send_master_setup_heartbeat)

          heartbeat = @redis.get('build:42:master-setup-heartbeat')
          refute_nil heartbeat, 'Heartbeat key should be set'
          assert_in_delta CI::Queue.time_now.to_f, heartbeat.to_f, 1.0
        end

        def test_send_master_setup_heartbeat_sets_ttl
          worker = create_worker(1)
          worker.send(:send_master_setup_heartbeat)

          ttl = @redis.ttl('build:42:master-setup-heartbeat')
          assert ttl > 0, 'Heartbeat should have TTL set'
        end

        # === acquire_master_role? Tests ===

        def test_acquire_master_role_sets_initial_heartbeat
          worker = create_worker(1)
          tests = TEST_LIST.map { |id| MockTest.new(id) }
          worker.populate(tests, random: Random.new(0))

          heartbeat = @redis.get('build:42:master-setup-heartbeat')
          refute_nil heartbeat, 'Initial heartbeat should be set after acquiring master'
        end

        def test_acquire_master_role_sets_master_worker_id
          worker = create_worker(1)
          tests = TEST_LIST.map { |id| MockTest.new(id) }
          worker.populate(tests, random: Random.new(0))

          master_id = @redis.get('build:42:master-worker-id')
          assert_equal '1', master_id
        end

        # === attempt_master_takeover Tests ===

        def test_attempt_master_takeover_fails_when_already_master
          worker = create_worker(1)
          tests = TEST_LIST.map { |id| MockTest.new(id) }
          worker.populate(tests, random: Random.new(0))

          refute worker.send(:attempt_master_takeover), 'Should not attempt takeover when already master'
        end

        def test_attempt_master_takeover_fails_when_heartbeat_fresh
          # Set up initial master
          @redis.set('build:42:master-status', 'setup')
          @redis.set('build:42:master-worker-id', '1')
          @redis.set('build:42:master-setup-heartbeat', CI::Queue.time_now.to_f)

          # Second worker tries to take over - should fail
          worker2 = create_worker(2, master_setup_heartbeat_timeout: 30)
          refute worker2.send(:attempt_master_takeover), 'Should not takeover when heartbeat is fresh'

          # Original master should still be set
          assert_equal '1', @redis.get('build:42:master-worker-id')
        end

        def test_attempt_master_takeover_succeeds_when_heartbeat_stale
          # Set up initial master with stale heartbeat
          @redis.set('build:42:master-status', 'setup')
          @redis.set('build:42:master-worker-id', '1')
          old_time = CI::Queue.time_now.to_f - 60
          @redis.set('build:42:master-setup-heartbeat', old_time)

          # Second worker tries to take over - should succeed
          worker2 = create_worker(2, master_setup_heartbeat_timeout: 30)
          assert worker2.send(:attempt_master_takeover), 'Should takeover when heartbeat is stale'

          # Worker 2 should now be master
          assert_equal '2', @redis.get('build:42:master-worker-id')
          assert worker2.master?, 'Worker 2 should be master after takeover'
        end

        def test_attempt_master_takeover_fails_when_status_not_setup
          # Set up master that completed setup
          @redis.set('build:42:master-status', 'ready')
          @redis.set('build:42:master-worker-id', '1')
          old_time = CI::Queue.time_now.to_f - 60
          @redis.set('build:42:master-setup-heartbeat', old_time)

          # Second worker tries to take over - should fail because status is 'ready'
          worker2 = create_worker(2, master_setup_heartbeat_timeout: 30)
          refute worker2.send(:attempt_master_takeover), 'Should not takeover when status is not setup'
        end

        def test_attempt_master_takeover_only_one_wins
          # Set up initial master with stale heartbeat
          @redis.set('build:42:master-status', 'setup')
          @redis.set('build:42:master-worker-id', '1')
          old_time = CI::Queue.time_now.to_f - 60
          @redis.set('build:42:master-setup-heartbeat', old_time)

          # Multiple workers try to take over simultaneously
          workers = (2..5).map { |i| create_worker(i, master_setup_heartbeat_timeout: 30) }

          results = workers.map { |w| w.send(:attempt_master_takeover) }

          # Exactly one should succeed
          assert_equal 1, results.count(true), 'Exactly one worker should win takeover'
        end

        # === with_master_setup_heartbeat Tests ===

        def test_with_master_setup_heartbeat_sends_heartbeats
          worker = create_worker(1, master_setup_heartbeat_interval: 0.1)

          heartbeat_times = []
          worker.send(:with_master_setup_heartbeat) do
            3.times do
              sleep 0.15
              heartbeat = @redis.get('build:42:master-setup-heartbeat')
              heartbeat_times << heartbeat.to_f if heartbeat
            end
          end

          # Should have received multiple heartbeats
          assert heartbeat_times.size >= 2, "Should have at least 2 heartbeat readings, got #{heartbeat_times.size}"
          # Heartbeats should be progressively newer
          assert heartbeat_times == heartbeat_times.sort, 'Heartbeats should be progressively newer'
        end

        def test_with_master_setup_heartbeat_stops_after_block
          worker = create_worker(1, master_setup_heartbeat_interval: 0.1)

          worker.send(:with_master_setup_heartbeat) do
            sleep 0.2
          end

          initial_heartbeat = @redis.get('build:42:master-setup-heartbeat').to_f
          sleep 0.3
          final_heartbeat = @redis.get('build:42:master-setup-heartbeat').to_f

          # Heartbeat should not have updated after block completed
          assert_equal initial_heartbeat, final_heartbeat, 'Heartbeat should stop updating after block'
        end

        # === wait_for_master with Takeover Tests ===

        def test_wait_for_master_triggers_takeover_when_master_dead
          # Set up a dead master (status = setup, but stale heartbeat)
          @redis.set('build:42:master-status', 'setup')
          @redis.set('build:42:master-worker-id', '1')
          old_time = CI::Queue.time_now.to_f - 60
          @redis.set('build:42:master-setup-heartbeat', old_time)

          # Worker 2 waits for master - should trigger takeover
          worker2 = create_worker(
            2,
            master_setup_heartbeat_timeout: 30,
            master_setup_heartbeat_interval: 0.1
          )
          tests = TEST_LIST.map { |id| MockTest.new(id) }

          # Populate will trigger wait_for_master for non-master workers
          # We need to simulate this by having populate fail to acquire master
          # but then wait_for_master should detect dead master and takeover

          # First, create the index without populating
          worker2.instance_variable_set(:@index, tests.map { |t| [t.id, t] }.to_h)
          worker2.instance_variable_set(:@total, tests.size)
          worker2.instance_variable_set(:@random, Random.new(0))
          worker2.instance_variable_set(:@tests, tests)
          worker2.instance_variable_set(:@master, false)

          # Now call wait_for_master - should detect dead master and take over
          result = worker2.wait_for_master(timeout: 5)

          assert result, 'wait_for_master should return true after takeover'
          assert worker2.master?, 'Worker 2 should be master after takeover'
          assert_equal '2', @redis.get('build:42:master-worker-id')
        end

        def test_wait_for_master_does_not_takeover_when_master_alive
          # Master worker sets up and sends heartbeats
          worker1 = create_worker(1)
          tests = TEST_LIST.map { |id| MockTest.new(id) }

          # Start master in background thread
          master_thread = Thread.new do
            worker1.populate(tests, random: Random.new(0))
          end

          # Worker 2 waits - should wait for master to finish, not take over
          worker2 = create_worker(
            2,
            master_setup_heartbeat_timeout: 30,
            master_setup_heartbeat_interval: 0.1,
            populate: false
          )

          result = worker2.wait_for_master(timeout: 5)
          master_thread.join

          assert result, 'wait_for_master should return true'
          refute worker2.master?, 'Worker 2 should not be master'
          assert worker1.master?, 'Worker 1 should be master'
        end

        # === Push with WATCH/MULTI Tests ===

        def test_push_aborts_when_master_id_changes
          worker1 = create_worker(1)
          tests = TEST_LIST.map { |id| MockTest.new(id) }

          # Set up worker1 as master
          worker1.instance_variable_set(:@master, true)
          worker1.instance_variable_set(:@index, tests.map { |t| [t.id, t] }.to_h)
          @redis.set('build:42:master-worker-id', '1')

          # Simulate race: another worker changed master-worker-id between WATCH and MULTI
          # We can't directly test the race, but we can verify the behavior
          # when master-worker-id doesn't match

          # Change master-worker-id before push
          @redis.set('build:42:master-worker-id', '2')

          worker1.send(:push, TEST_LIST)

          # Worker1 should have detected the race and aborted
          refute worker1.master?, 'Worker1 should lose master role after race detection'
        end

        def test_push_succeeds_when_master_id_matches
          worker1 = create_worker(1)
          tests = TEST_LIST.map { |id| MockTest.new(id) }

          # Actually populate to set up master correctly
          worker1.populate(tests, random: Random.new(0))

          assert worker1.master?, 'Worker1 should be master'
          assert_equal 'ready', @redis.get('build:42:master-status')
          assert_equal TEST_LIST.size, @redis.llen('build:42:queue')
        end

        # === execute_master_setup Tests ===

        def test_execute_master_setup_runs_with_heartbeat
          worker = create_worker(1, master_setup_heartbeat_interval: 0.1)
          tests = TEST_LIST.map { |id| MockTest.new(id) }

          # Set up worker as master with required state
          worker.instance_variable_set(:@master, true)
          worker.instance_variable_set(:@index, tests.map { |t| [t.id, t] }.to_h)
          worker.instance_variable_set(:@tests, tests)
          worker.instance_variable_set(:@random, Random.new(0))
          @redis.set('build:42:master-worker-id', '1')

          worker.send(:execute_master_setup)

          # Queue should be populated
          assert_equal 'ready', @redis.get('build:42:master-status')
          assert_equal TEST_LIST.size, @redis.llen('build:42:queue')
        end

        def test_execute_master_setup_does_nothing_when_not_master
          worker = create_worker(1)
          tests = TEST_LIST.map { |id| MockTest.new(id) }

          worker.instance_variable_set(:@master, false)
          worker.instance_variable_set(:@index, tests.map { |t| [t.id, t] }.to_h)

          worker.send(:execute_master_setup)

          # Queue should not be populated
          assert_nil @redis.get('build:42:master-status')
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
          skip_populate = options.delete(:populate) == false
          config_options = {
            build_id: '42',
            worker_id: id.to_s,
            timeout: 0.2,
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
  end
end
