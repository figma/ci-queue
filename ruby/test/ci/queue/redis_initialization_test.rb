# frozen_string_literal: true

require 'test_helper'

class CI::Queue::Redis::InitializationTest < Minitest::Test
  include QueueHelper

  def setup
    @redis_url = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
    @redis = ::Redis.new(url: @redis_url)
    @redis.flushdb
  end

  def teardown
    @redis.flushdb
  end

  def test_surviving_worker_recovers_when_initializer_exits
    exit_during_setup(worker_id: 0)

    survivor = worker(1).populate(tests)
    refute_predicate survivor, :master?
    assert_equal tests.sort, poll(survivor).sort
    assert_predicate survivor, :master?
    assert_equal '1', @redis.get(key('master-worker-id'))
    assert_equal 'ready', @redis.get(key('master-status'))
  end

  def test_retried_initializer_recovers_its_own_setup_marker
    exit_during_setup(worker_id: 0)

    retried = worker(0).populate(tests)
    assert_equal tests.sort, poll(retried).sort
    assert_predicate retried, :master?
    assert_equal 'ready', @redis.get(key('master-status'))
  end

  def test_worker_takes_over_after_setup_exception
    initializer = worker(0)
    initializer.define_singleton_method(:reorder_tests) { |*| raise 'setup failed' }

    error = assert_raises(RuntimeError) { initializer.populate(tests) }
    assert_equal 'setup failed', error.message

    replacement = worker(1).populate(tests)
    assert_equal tests.sort, poll(replacement).sort
    assert_predicate replacement, :master?
  end

  def test_late_initializer_cannot_publish_over_replacement
    initializer = worker(0)
    replacement = worker(1)
    redis = @redis
    status_key = key('master-status')
    initializer.define_singleton_method(:reorder_tests) do |input, **|
      redis.del(status_key)
      replacement.populate(input)
      input.reverse
    end
    initializer.populate(tests)

    assert_predicate replacement, :master?
    refute_predicate initializer, :master?
    assert_equal tests.size, @redis.llen(key('queue'))
    assert_equal tests.sort, poll(replacement).sort
    assert_equal '1', @redis.get(key('master-worker-id'))
  end

  def test_late_initializer_cannot_overwrite_chunk_metadata
    initializer = worker(0)
    replacement = worker(1, strategy: :suite_bin_packing)
    redis = @redis
    status_key = key('master-status')
    chunk_key = key('chunk:ATest:chunk_0')
    published_metadata = nil
    initializer.define_singleton_method(:reorder_tests) do |input, **|
      redis.del(status_key)
      replacement.populate(input)
      published_metadata = redis.get(chunk_key)
      [CI::Queue::TestChunk.new('ATest:chunk_0', 'ATest', [input.first.id], 9999)]
    end
    initializer.populate(tests)

    refute_nil published_metadata
    assert_equal published_metadata, @redis.get(chunk_key)
    refute_predicate initializer, :master?
    assert_equal tests.sort, poll(replacement).flat_map(&:tests).sort
  end

  def test_waiting_worker_does_not_take_over_a_live_setup
    initializer = worker(0)
    started = Queue.new
    proceed = Queue.new
    initializer.define_singleton_method(:reorder_tests) do |input, **|
      started << true
      proceed.pop
      input
    end
    thread = Thread.new { initializer.populate(tests) }
    started.pop

    contender = worker(1).populate(tests)
    refute_predicate contender, :master?
    assert_equal 'setup', @redis.get(key('master-status'))

    proceed << true
    assert thread.join(2), 'initializer did not complete'
    assert_predicate initializer, :master?
    assert_equal tests.sort, poll(contender).sort
    refute_predicate contender, :master?
  ensure
    proceed << true if proceed
    thread&.kill
    thread&.join
  end

  private

  def exit_during_setup(worker_id:)
    child = fork do
      initializer = worker(worker_id)
      initializer.define_singleton_method(:reorder_tests) { |*| exit! 143 }
      initializer.populate(tests)
    end
    _, status = Process.wait2(child)

    assert_equal 143, status.exitstatus
    assert_equal 'setup', @redis.get(key('master-status'))
    assert_operator @redis.pttl(key('master-status')), :>, 0
  end

  def tests
    SharedQueueAssertions::TEST_LIST.dup
  end

  def key(name)
    "build:initialization:#{name}"
  end

  def worker(id, **options)
    CI::Queue::Redis.new(
      @redis_url,
      CI::Queue::Configuration.new(
        build_id: 'initialization',
        worker_id: id.to_s,
        timeout: 0.5,
        queue_init_timeout: 0.5,
        heartbeat_interval: 0,
        **options,
      ),
    )
  end
end
