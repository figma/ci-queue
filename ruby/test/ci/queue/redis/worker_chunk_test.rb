# frozen_string_literal: true
require 'test_helper'

class CI::Queue::WorkerChunkTest < Minitest::Test
  def setup
    @redis_url = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
    @redis = ::Redis.new(url: @redis_url)
    @redis.flushdb

    @config = CI::Queue::Configuration.new(
      build_id: '42',
      worker_id: '1',
      timeout: 0.2,
      strategy: :suite_bin_packing,
      suite_max_duration: 120_000,
      timing_fallback_duration: 100.0
    )

    @worker = CI::Queue::Redis.new(@redis_url, @config)
  end

  def teardown
    @redis.flushdb if @redis
  end

  def test_populate_stores_chunk_metadata_in_redis
    tests = create_mock_tests(['TestA#test_1', 'TestA#test_2'])
    chunks = [
      CI::Queue::TestChunk.new('TestA:full_suite', 'TestA', :full_suite, [], 5000.0)
    ]

    # Simulate strategy returning chunks
    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    # Verify chunk metadata was stored
    chunk_data = @redis.get('build:42:chunk:TestA:full_suite')
    refute_nil chunk_data

    parsed = JSON.parse(chunk_data)
    assert_equal 'full_suite', parsed['type']
    assert_equal 'TestA', parsed['suite_name']
    assert_equal 5000.0, parsed['estimated_duration']
  end

  def test_populate_stores_partial_suite_with_test_ids
    tests = create_mock_tests(['TestA#test_1', 'TestA#test_2'])
    test_ids = ['TestA#test_1', 'TestA#test_2']
    chunks = [
      CI::Queue::TestChunk.new('TestA:chunk_0', 'TestA', :partial_suite, test_ids, 3000.0)
    ]

    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    chunk_data = @redis.get('build:42:chunk:TestA:chunk_0')
    refute_nil chunk_data

    parsed = JSON.parse(chunk_data)
    assert_equal 'partial_suite', parsed['type']
    assert_equal test_ids, parsed['test_ids']
  end

  def test_chunk_id_detection
    assert @worker.send(:chunk_id?, 'TestA:full_suite')
    assert @worker.send(:chunk_id?, 'TestB:chunk_0')
    assert @worker.send(:chunk_id?, 'TestC:chunk_5')
    refute @worker.send(:chunk_id?, 'TestA#test_method')
    refute @worker.send(:chunk_id?, 'SimpleTest')
  end

  def test_resolve_executable_for_single_test
    tests = create_mock_tests(['TestA#test_1'])
    @worker.populate(tests)

    executable = @worker.send(:resolve_executable, 'TestA#test_1')

    assert_kind_of MockTest, executable
    assert_equal 'TestA#test_1', executable.id
  end

  def test_resolve_full_suite_chunk
    tests = create_mock_tests(['TestA#test_1', 'TestA#test_2', 'TestA#test_3'])
    chunks = [
      CI::Queue::TestChunk.new('TestA:full_suite', 'TestA', :full_suite, [], 3000.0)
    ]

    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    # Manually store and resolve chunk
    resolved = @worker.send(:resolve_chunk, 'TestA:full_suite')

    assert_instance_of CI::Queue::Redis::Worker::ResolvedChunk, resolved
    assert_equal 'TestA:full_suite', resolved.id
    assert_equal 'TestA', resolved.suite_name
    assert_equal 3, resolved.tests.size
    assert resolved.tests.all? { |t| t.id.start_with?('TestA#') }
  end

  def test_resolve_partial_suite_chunk
    tests = create_mock_tests(['TestA#test_1', 'TestA#test_2', 'TestA#test_3'])
    test_ids = ['TestA#test_1', 'TestA#test_3']
    chunks = [
      CI::Queue::TestChunk.new('TestA:chunk_0', 'TestA', :partial_suite, test_ids, 2000.0)
    ]

    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    resolved = @worker.send(:resolve_chunk, 'TestA:chunk_0')

    assert_instance_of CI::Queue::Redis::Worker::ResolvedChunk, resolved
    assert_equal 'TestA:chunk_0', resolved.id
    assert_equal 2, resolved.tests.size
    assert_equal ['TestA#test_1', 'TestA#test_3'], resolved.tests.map(&:id)
  end

  def test_resolve_chunk_returns_nil_for_missing_metadata
    resolved = @worker.send(:resolve_chunk, 'NonexistentChunk:full_suite')
    assert_nil resolved
  end

  def test_resolved_chunk_interface
    tests = create_mock_tests(['TestA#test_1', 'TestA#test_2'])
    chunk = CI::Queue::TestChunk.new('TestA:full_suite', 'TestA', :full_suite, [], 2000.0)

    resolved = CI::Queue::Redis::Worker::ResolvedChunk.new(chunk, tests)

    assert_equal 'TestA:full_suite', resolved.id
    assert_equal 'TestA', resolved.suite_name
    assert_equal 2, resolved.size
    assert resolved.chunk?
    assert_equal tests, resolved.tests
    refute resolved.flaky?
  end

  def test_resolved_chunk_detects_flaky_tests
    tests = create_mock_tests(['TestA#test_1', 'TestA#test_2'])
    tests.first.stub(:flaky?, true) do
      chunk = CI::Queue::TestChunk.new('TestA:full_suite', 'TestA', :full_suite, [], 2000.0)
      resolved = CI::Queue::Redis::Worker::ResolvedChunk.new(chunk, tests)

      assert resolved.flaky?
    end
  end

  def test_acknowledge_chunk
    # Set up a chunk as if it were reserved (in running zset)
    chunk_id = 'TestA:full_suite'
    @redis.zadd('build:42:running', Time.now.to_i, chunk_id)
    @redis.hset('build:42:owners', chunk_id, 'build:42:worker:1:queue')
    @worker.instance_variable_set(:@reserved_test, chunk_id)

    # Acknowledge the chunk
    result = @worker.acknowledge(chunk_id)

    # Verify chunk was removed from running and added to processed
    assert result
    refute @redis.zrank('build:42:running', chunk_id)
    assert @redis.sismember('build:42:processed', chunk_id)
    refute @redis.hexists('build:42:owners', chunk_id)
  end

  def test_populate_with_mixed_chunks_and_tests
    tests = create_mock_tests([
      'TestA#test_1',
      'TestB#test_1',
      'TestB#test_2'
    ])

    chunks = [
      CI::Queue::TestChunk.new('TestA:full_suite', 'TestA', :full_suite, [], 1000.0),
      tests[1] # Individual test
    ]

    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    # Check that both chunk and individual test IDs are in queue
    queue_items = @redis.lrange('build:42:queue', 0, -1)
    assert_includes queue_items, 'TestA:full_suite'
    assert_includes queue_items, 'TestB#test_1'
  end

  def test_chunk_metadata_has_ttl
    tests = create_mock_tests(['TestA#test_1'])
    chunks = [
      CI::Queue::TestChunk.new('TestA:full_suite', 'TestA', :full_suite, [], 1000.0)
    ]

    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    ttl = @redis.ttl('build:42:chunk:TestA:full_suite')
    assert ttl > 0, 'Chunk metadata should have TTL set'
  end

  def test_populate_with_many_chunks_uses_batching
    # Create 20 test chunks to verify batching works (batch size is 7)
    tests = (1..20).map { |i| MockTest.new("TestSuite#{i}#test_1") }
    chunks = (1..20).map do |i|
      CI::Queue::TestChunk.new("TestSuite#{i}:full_suite", "TestSuite#{i}", :full_suite, [], 1000.0)
    end

    @worker.stub(:reorder_tests, chunks) do
      @worker.populate(tests)
    end

    # Verify all chunks were stored despite batching
    chunks.each do |chunk|
      chunk_data = @redis.get("build:42:chunk:#{chunk.id}")
      refute_nil chunk_data, "Chunk #{chunk.id} should be stored"

      parsed = JSON.parse(chunk_data)
      assert_equal 'full_suite', parsed['type']
      assert_equal chunk.suite_name, parsed['suite_name']
    end

    # Verify all chunk IDs are in the chunks set
    stored_chunks = @redis.smembers('build:42:chunks')
    chunks.each do |chunk|
      assert_includes stored_chunks, chunk.id
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
  end
end
