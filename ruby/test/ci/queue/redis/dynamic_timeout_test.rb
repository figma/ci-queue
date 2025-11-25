# frozen_string_literal: true

require 'test_helper'

class CI::Queue::DynamicTimeoutTest < Minitest::Test
  def setup
    @redis_url = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
    @redis = ::Redis.new(url: @redis_url)
    @redis.flushdb

    @config = CI::Queue::Configuration.new(
      build_id: '42',
      worker_id: '1',
      timeout: 30, # 30 seconds default timeout
      strategy: :suite_bin_packing,
      suite_max_duration: 120_000,
      timing_fallback_duration: 100.0,
      timing_redis_url: @redis_url
    )

    @worker = CI::Queue::Redis.new(@redis_url, @config)
  end

  def teardown
    @redis.flushdb if @redis
  end

  def test_chunk_timeout_stored_in_redis_hash
    tests = create_mock_tests(['TestA#test_1', 'TestA#test_2', 'TestA#test_3'])
    test_ids = ['TestA#test_1', 'TestA#test_2', 'TestA#test_3']
    chunks = [
      CI::Queue::TestChunk.new('TestA:chunk_0', 'TestA', test_ids, 5000.0, test_count: 3)
    ]

    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    # Verify timeout was stored in test-group-timeout hash
    # Timeout should be: default_timeout (30s) * number_of_tests (3) = 90s
    chunk_timeout = @redis.hget('build:42:test-group-timeout', 'TestA:chunk_0')
    refute_nil chunk_timeout
    assert_equal '90', chunk_timeout
  end

  def test_chunk_timeout_scales_with_test_count
    # Small chunk: 5 tests
    small_test_ids = (1..5).map { |i| "SmallSuite#test_#{i}" }
    small_tests = create_mock_tests(small_test_ids)
    small_chunk = CI::Queue::TestChunk.new('SmallSuite:chunk_0', 'SmallSuite', small_test_ids, 1000.0, test_count: 5)

    worker = CI::Queue::Redis.new(@redis_url, @config)
    worker.stub(:reorder_tests, [small_chunk]) do
      worker.populate(small_tests)
    end

    small_timeout = @redis.hget('build:42:test-group-timeout', 'SmallSuite:chunk_0')
    assert_equal '150', small_timeout # 30s * 5 tests

    @redis.flushdb

    # Large chunk: 20 tests
    large_test_ids = (1..20).map { |i| "LargeSuite#test_#{i}" }
    large_tests = create_mock_tests(large_test_ids)
    large_chunk = CI::Queue::TestChunk.new('LargeSuite:chunk_0', 'LargeSuite', large_test_ids, 5000.0, test_count: 20)

    worker = CI::Queue::Redis.new(@redis_url, @config)
    worker.stub(:reorder_tests, [large_chunk]) do
      worker.populate(large_tests)
    end

    large_timeout = @redis.hget('build:42:test-group-timeout', 'LargeSuite:chunk_0')
    assert_equal '600', large_timeout # 30s * 20 tests
  end

  def test_multiple_chunks_stored_with_different_timeouts
    tests = create_mock_tests([
                                'TestA#test_1', 'TestA#test_2', # 2 tests
                                'TestB#test_1', 'TestB#test_2', 'TestB#test_3', # 3 tests
                                'TestC#test_1' # 1 test
                              ])

    chunks = [
      CI::Queue::TestChunk.new('TestA:chunk_0', 'TestA', ['TestA#test_1', 'TestA#test_2'], 2000.0, test_count: 2),
      CI::Queue::TestChunk.new('TestB:chunk_0', 'TestB', ['TestB#test_1', 'TestB#test_2', 'TestB#test_3'], 3000.0,
                               test_count: 3),
      CI::Queue::TestChunk.new('TestC:chunk_0', 'TestC', ['TestC#test_1'], 1000.0, test_count: 1)
    ]

    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    # Verify each chunk has correct timeout
    assert_equal '60', @redis.hget('build:42:test-group-timeout', 'TestA:chunk_0') # 30s * 2
    assert_equal '90', @redis.hget('build:42:test-group-timeout', 'TestB:chunk_0') # 30s * 3
    assert_equal '30', @redis.hget('build:42:test-group-timeout', 'TestC:chunk_0') # 30s * 1
  end

  def test_timeout_hash_has_ttl
    tests = create_mock_tests(['TestA#test_1', 'TestA#test_2'])
    test_ids = ['TestA#test_1', 'TestA#test_2']
    chunks = [
      CI::Queue::TestChunk.new('TestA:chunk_0', 'TestA', test_ids, 1000.0, test_count: 2)
    ]

    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    ttl = @redis.ttl('build:42:test-group-timeout')
    assert ttl > 0, 'test-group-timeout hash should have TTL set'
    assert ttl <= @config.redis_ttl, 'TTL should not exceed config.redis_ttl'
  end

  def test_single_test_not_in_timeout_hash
    tests = create_mock_tests(['TestA#test_1', 'TestB#test_1'])

    # Return individual tests, not chunks
    @worker.stub(:reorder_tests, tests) do
      @worker.populate(tests)
    end

    # Verify individual tests are NOT in the timeout hash
    assert_nil @redis.hget('build:42:test-group-timeout', 'TestA#test_1')
    assert_nil @redis.hget('build:42:test-group-timeout', 'TestB#test_1')
  end

  def test_mixed_chunks_and_tests_only_chunks_have_timeouts
    tests = create_mock_tests([
                                'TestA#test_1', 'TestA#test_2',
                                'TestB#test_1',
                                'TestC#test_1', 'TestC#test_2', 'TestC#test_3'
                              ])

    chunks = [
      CI::Queue::TestChunk.new('TestA:chunk_0', 'TestA', ['TestA#test_1', 'TestA#test_2'], 2000.0, test_count: 2),
      tests[2], # Individual test TestB#test_1
      CI::Queue::TestChunk.new('TestC:chunk_0', 'TestC', ['TestC#test_1', 'TestC#test_2', 'TestC#test_3'], 3000.0,
                               test_count: 3)
    ]

    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    # Chunks should have timeouts
    assert_equal '60', @redis.hget('build:42:test-group-timeout', 'TestA:chunk_0')
    assert_equal '90', @redis.hget('build:42:test-group-timeout', 'TestC:chunk_0')

    # Individual test should not
    assert_nil @redis.hget('build:42:test-group-timeout', 'TestB#test_1')
  end

  def test_reserve_test_passes_dynamic_deadline_flag
    tests = create_mock_tests(['TestA#test_1'])

    @worker.stub(:reorder_tests, tests) do
      @worker.populate(tests)
    end

    # Mock the eval_script call to verify correct parameters
    expected_keys = [
      'build:42:queue',
      'build:42:running',
      'build:42:processed',
      'build:42:worker:1:queue',
      'build:42:owners',
      'build:42:test-group-timeout' # 6th key for dynamic deadline
    ]

    @worker.stub(:eval_script, proc { |script, keys:, argv:|
      assert_equal :reserve, script
      assert_equal expected_keys, keys
      assert_equal 3, argv.length
      assert_instance_of Float, argv[0]
      assert_equal 'true', argv[1]
      assert_equal @config.timeout, argv[2]
      nil # Return nil (no test reserved)
    }) do
      @worker.send(:try_to_reserve_test)
    end
  end

  def test_reserve_lost_test_passes_dynamic_deadline_flag
    tests = create_mock_tests(['TestA#test_1'])

    @worker.stub(:reorder_tests, tests) do
      @worker.populate(tests)
    end

    expected_keys = [
      'build:42:running',
      'build:42:completed',
      'build:42:worker:1:queue',
      'build:42:owners',
      'build:42:test-group-timeout' # 5th key for dynamic deadline
    ]

    @worker.stub(:eval_script, proc { |script, keys:, argv:|
      assert_equal :reserve_lost, script
      assert_equal expected_keys, keys
      assert_equal 4, argv.length
      assert_instance_of Float, argv[0]
      assert_equal @config.timeout, argv[1]
      assert_equal 'true', argv[2]
      assert_equal @config.timeout, argv[3]
      nil # Return nil (no lost test)
    }) do
      @worker.send(:try_to_reserve_lost_test)
    end
  end

  def test_chunk_not_marked_lost_before_dynamic_timeout
    # Create worker with short timeout for faster test
    config = CI::Queue::Configuration.new(
      build_id: 'timeout-test',
      worker_id: '1',
      timeout: 0.5, # 0.5 seconds
      strategy: :suite_bin_packing
    )

    worker1 = CI::Queue::Redis.new(@redis_url, config)

    # Create chunk with 5 tests -> timeout = 0.5s * 5 = 2.5s
    test_ids = (1..5).map { |i| "TestSuite#test_#{i}" }
    tests = create_mock_tests(test_ids)
    chunk = CI::Queue::TestChunk.new('TestSuite:chunk_0', 'TestSuite', test_ids, 5000.0, test_count: 5)

    worker1.stub(:reorder_tests, [chunk]) do
      worker1.populate(tests)
    end

    # Reserve the chunk with worker1
    reserved_id = worker1.send(:try_to_reserve_test)
    assert_equal 'TestSuite:chunk_0', reserved_id

    # Wait 1 second (less than 2.5s timeout)
    sleep 1

    # Try to reserve with worker2 - should not get the chunk (not lost yet)
    worker2_config = config.dup
    worker2_config.instance_variable_set(:@worker_id, '2')
    worker2 = CI::Queue::Redis.new(@redis_url, worker2_config)

    lost_test = worker2.send(:try_to_reserve_lost_test)
    assert_nil lost_test, 'Chunk should not be marked as lost before dynamic timeout'
  end

  def test_single_test_marked_lost_after_default_timeout
    # Create worker with short timeout
    config = CI::Queue::Configuration.new(
      build_id: 'single-timeout-test',
      worker_id: '1',
      timeout: 0.5 # 0.5 seconds
    )

    worker1 = CI::Queue::Redis.new(@redis_url, config)

    # Populate with single test (no chunk)
    tests = create_mock_tests(['TestA#test_1'])

    worker1.stub(:reorder_tests, tests) do
      worker1.populate(tests)
    end

    # Reserve the test with worker1
    reserved_id = worker1.send(:try_to_reserve_test)
    assert_equal 'TestA#test_1', reserved_id

    # Wait longer than timeout (0.5s)
    sleep 0.6

    # Try to reserve with worker2 - should get the lost test
    worker2_config = config.dup
    worker2_config.instance_variable_set(:@worker_id, '2')
    worker2 = CI::Queue::Redis.new(@redis_url, worker2_config)

    lost_test = worker2.send(:try_to_reserve_lost_test)
    assert_equal 'TestA#test_1', lost_test, 'Single test should be marked as lost after default timeout'
  end

  def test_batching_with_many_chunks
    # Create 15 chunks to test batching (batch size is 5)
    tests = (1..15).map { |i| MockTest.new("TestSuite#{i}#test_1") }
    chunks = (1..15).map do |i|
      test_ids = ["TestSuite#{i}#test_1"]
      CI::Queue::TestChunk.new("TestSuite#{i}:chunk_0", "TestSuite#{i}", test_ids, 1000.0, test_count: 1)
    end

    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    # Verify all chunks have timeouts stored despite batching
    chunks.each do |chunk|
      timeout = @redis.hget('build:42:test-group-timeout', chunk.id)
      refute_nil timeout, "Chunk #{chunk.id} should have timeout stored"
      assert_equal '30', timeout # 30s * 1 test
    end
  end

  private

  def create_mock_tests(test_ids)
    test_ids.map { |id| MockTest.new(id) }
  end

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
end
