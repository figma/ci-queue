# frozen_string_literal: true

require 'test_helper'

class CI::Queue::Redis::MasterRecoveryTest < Minitest::Test
  include QueueHelper

  BUILD_ID = 'master-recovery'
  TEST_IDS = %w[
    ATest#test_foo
    ATest#test_bar
    BTest#test_foo
    BTest#test_bar
  ].freeze

  MockTest = Struct.new(:id) do
    def <=>(other)
      id <=> other.id
    end

    def flaky?
      false
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

  def test_surviving_worker_repopulates_after_master_dies_during_setup
    # Model a worker that won election and was terminated before it could
    # publish the queue. The lease expires without any process cleaning it up.
    @redis.set(master_status_key, 'setup:dead-generation', ex: 1)

    survivor = worker('survivor')
    survivor.populate(tests, random: Random.new(0))

    assert_predicate survivor, :master?
    refute_equal 'dead-generation', @redis.get(current_generation_key)

    executed = poll(survivor).map(&:id)
    assert_equal TEST_IDS.sort, executed.sort
    assert_equal TEST_IDS.length, executed.length
  end

  def test_stale_master_cannot_overwrite_replacement_queue
    dead_master = worker('dead-master', master_lock_ttl: 1)
    assert dead_master.send(:acquire_master_role?)
    dead_generation = dead_master.instance_variable_get(:@generation)

    sleep 1.1

    survivor = worker('survivor', master_lock_ttl: 1)
    survivor.populate(tests, random: Random.new(0))
    replacement_generation = @redis.get(current_generation_key)
    queue_before_stale_push = @redis.lrange("build:#{BUILD_ID}:queue", 0, -1)

    assert_raises(CI::Queue::Redis::MasterDied) do
      dead_master.send(:push, ['StaleTest#test_should_not_run'])
    end

    refute_equal dead_generation, replacement_generation
    assert_equal replacement_generation, @redis.get(current_generation_key)
    assert_equal queue_before_stale_push, @redis.lrange("build:#{BUILD_ID}:queue", 0, -1)
    refute_includes queue_before_stale_push, 'StaleTest#test_should_not_run'
  end

  def test_master_renews_lease_during_slow_population
    master = worker('slow-master', master_lock_ttl: 1)

    slow_reorder = lambda do |passed_tests, **_args|
      sleep 1.2
      passed_tests
    end
    master.stub(:reorder_tests, slow_reorder) do
      master.populate(tests, random: Random.new(0))
    end

    assert_predicate master, :master?
    assert_equal 'ready', @redis.get(master_status_key)
    refute_nil @redis.get(current_generation_key)
  end

  private

  def current_generation_key
    "build:#{BUILD_ID}:current-generation"
  end

  def master_status_key
    "build:#{BUILD_ID}:master-status"
  end

  def tests
    TEST_IDS.map { |id| MockTest.new(id) }
  end

  def worker(id, **options)
    CI::Queue::Redis.new(
      @redis_url,
      CI::Queue::Configuration.new(
        build_id: BUILD_ID,
        worker_id: id,
        timeout: 0.2,
        queue_init_timeout: 3,
        timing_redis_url: @redis_url,
        **options,
      ),
    )
  end
end
