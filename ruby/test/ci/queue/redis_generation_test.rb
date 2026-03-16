# frozen_string_literal: true

require 'test_helper'

class CI::Queue::Redis::GenerationTest < Minitest::Test
  include QueueHelper

  TEST_LIST = %w[
    ATest#test_foo
    ATest#test_bar
    BTest#test_foo
    BTest#test_bar
  ].freeze

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

  def setup
    @redis_url = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
    @redis = ::Redis.new(url: @redis_url)
    @redis.flushdb
  end

  def teardown
    @redis.flushdb
  end

  def test_normal_election_and_generation_scoping
    w1 = worker(1, build_id: 'gen-1')
    assert_predicate w1, :master?

    gen_keys = @redis.keys('*gen:*')
    assert gen_keys.any?, "Expected generation-scoped Redis keys, got: #{@redis.keys('*').inspect}"

    w2 = worker(2, build_id: 'gen-1')
    refute_predicate w2, :master?

    tests_seen = []
    w1.poll do |test|
      tests_seen << test.id
      w1.acknowledge(test)
    end
    w2.poll do |test|
      tests_seen << test.id
      w2.acknowledge(test)
    end

    assert_equal TEST_LIST.sort, tests_seen.sort
  end

  def test_acknowledged_test_not_re_stolen
    w1 = worker(1, build_id: 'ack-1', timeout: 0.2)

    first_test = nil
    w1.poll do |test|
      first_test = test
      w1.acknowledge(test)
      break
    end
    refute_nil first_test

    sleep 0.3

    w2 = worker(2, build_id: 'ack-1', timeout: 0.2)
    stolen_tests = []
    w2.poll do |test|
      stolen_tests << test.id
      w2.acknowledge(test)
    end

    refute_includes stolen_tests, first_test.id,
      "Acknowledged test should not be re-stolen"
  end

  def test_master_death_detection
    build_id = 'death-1'
    @redis.set("build:#{build_id}:master-status", "setup:fake-gen", ex: 1)

    supervisor = CI::Queue::Redis::Supervisor.new(
      @redis_url,
      CI::Queue::Configuration.new(
        build_id: build_id,
        timeout: 0.2,
        queue_init_timeout: 2,
        timing_redis_url: @redis_url,
      ),
    )

    assert_raises(CI::Queue::Redis::LostMaster, CI::Queue::Redis::MasterDied) do
      supervisor.wait_for_master(timeout: 2)
    end
  end

  def test_election_retry_on_master_death
    build_id = 'retry-1'
    # Pre-set master-status so first election NX fails, but TTL is short
    @redis.set("build:#{build_id}:master-status", "setup:fake-gen", ex: 1)

    w = CI::Queue::Redis.new(
      @redis_url,
      CI::Queue::Configuration.new(
        build_id: build_id,
        worker_id: '1',
        timeout: 0.2,
        master_lock_ttl: 1,
        max_election_attempts: 3,
        queue_init_timeout: 3,
        timing_redis_url: @redis_url,
      ),
    )

    tests = TEST_LIST.map { |id| MockTest.new(id) }
    # Lock expires after 1s, worker retries election and becomes master
    result = w.populate(tests, random: Random.new(0))
    assert_equal w, result
    assert_predicate w, :master?
  end

  def test_cas_push_rejection
    build_id = 'cas-1'
    w = CI::Queue::Redis.new(
      @redis_url,
      CI::Queue::Configuration.new(
        build_id: build_id,
        worker_id: '1',
        timeout: 0.2,
        master_lock_ttl: 30,
        timing_redis_url: @redis_url,
      ),
    )

    # Acquire master role manually
    assert w.send(:acquire_master_role?)

    # Overwrite master-status to simulate another master taking over
    @redis.set("build:#{build_id}:master-status", "setup:other-gen")

    assert_raises(CI::Queue::Redis::MasterDied) do
      w.send(:push, %w[test1 test2])
    end
  end

  def test_generation_staleness_detection
    w1 = worker(1, build_id: 'stale-1')

    # w1 is master, create a non-master worker that learns generation
    w2 = worker(2, build_id: 'stale-1')
    refute_predicate w2, :master?

    # Overwrite current-generation in Redis
    @redis.set("build:stale-1:current-generation", "different-uuid")

    # Force staleness check by clearing the throttle
    w2.instance_variable_set(:@last_generation_check_at, nil)
    assert w2.send(:generation_stale?), "Expected generation to be stale after overwrite"
  end

  def test_supervisor_with_generations
    w = worker(1, build_id: 'sup-1')

    supervisor = CI::Queue::Redis::Supervisor.new(
      @redis_url,
      CI::Queue::Configuration.new(
        build_id: 'sup-1',
        timeout: 5,
        timing_redis_url: @redis_url,
      ),
    )

    assert_equal TEST_LIST.size, supervisor.total
  end

  def test_max_election_attempts_exceeded
    build_id = 'max-elect-1'
    # Set master-status with long TTL so it never expires during test
    @redis.set("build:#{build_id}:master-status", "setup:fake-gen", ex: 60)

    w = CI::Queue::Redis.new(
      @redis_url,
      CI::Queue::Configuration.new(
        build_id: build_id,
        worker_id: '1',
        timeout: 0.2,
        master_lock_ttl: 60,
        max_election_attempts: 1,
        queue_init_timeout: 1,
        timing_redis_url: @redis_url,
      ),
    )

    tests = TEST_LIST.map { |id| MockTest.new(id) }
    assert_raises(CI::Queue::Redis::LostMaster) do
      w.populate(tests, random: Random.new(0))
    end
  end

  private

  def worker(id, build_id: '42', timeout: 0.2, **args)
    tests = args.delete(:tests) || TEST_LIST.map { |tid| MockTest.new(tid) }
    queue = CI::Queue::Redis.new(
      @redis_url,
      CI::Queue::Configuration.new(
        build_id: build_id,
        worker_id: id.to_s,
        timeout: timeout,
        timing_redis_url: @redis_url,
        **args,
      ),
    )
    queue.populate(tests, random: Random.new(0))
  end
end
