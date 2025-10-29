# frozen_string_literal: true
require 'test_helper'
require 'tempfile'

require 'minitest/focus'

class SuiteBinPackingTest < Minitest::Test
  def setup
    @config = CI::Queue::Configuration.new(
      suite_max_duration: 120_000,
      suite_buffer_percent: 10,
      timing_fallback_duration: 100.0
    )
    @strategy = CI::Queue::Strategy::SuiteBinPacking.new(@config)
  end

  def test_groups_tests_by_suite
    tests = create_mock_tests([
      'UserTest#test_1',
      'UserTest#test_2',
      'OrderTest#test_1'
    ])

    chunks = @strategy.order_tests(tests)

    suite_names = chunks.map(&:suite_name).uniq
    assert_includes suite_names, 'UserTest'
    assert_includes suite_names, 'OrderTest'
  end

  def test_creates_full_suite_chunk_when_under_max_duration
    tests = create_mock_tests(['SmallTest#test_1', 'SmallTest#test_2'])
    timing_data = {
      'SmallTest#test_1' => 1000.0,
      'SmallTest#test_2' => 2000.0
    }

    chunks = order_with_timing(tests, timing_data)

    chunk = chunks.find { |c| c.suite_name == 'SmallTest' }
    assert chunk.full_suite?
    assert_equal 3000.0, chunk.estimated_duration
    assert_equal [], chunk.test_ids
  end

  def test_splits_suite_when_over_max_duration
    tests = create_mock_tests([
      'LargeTest#test_1',
      'LargeTest#test_2',
      'LargeTest#test_3',
      'LargeTest#test_4'
    ])
    timing_data = {
      'LargeTest#test_1' => 60_000.0,
      'LargeTest#test_2' => 50_000.0,
      'LargeTest#test_3' => 40_000.0,
      'LargeTest#test_4' => 30_000.0
    }

    chunks = order_with_timing(tests, timing_data)
    large_test_chunks = chunks.select { |c| c.suite_name == 'LargeTest' }

    assert large_test_chunks.size > 1, 'Should split into multiple chunks'
    large_test_chunks.each do |chunk|
      assert chunk.partial_suite?
      assert chunk.test_ids.any?, 'Partial suite should have test_ids'
    end
  end

  def test_applies_buffer_when_splitting
    @config.suite_max_duration = 100_000
    @config.suite_buffer_percent = 10

    tests = create_mock_tests(['TestSuite#test_1', 'TestSuite#test_2'])
    timing_data = {
      'TestSuite#test_1' => 50_000.0,
      'TestSuite#test_2' => 45_000.0
    }

    chunks = order_with_timing(tests, timing_data)

    # Total is 95,000 which is under 100,000 but over 90,000 (with 10% buffer)
    # So it should still fit in one chunk since total < max_duration
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }
    assert_equal 1, test_suite_chunks.size
  end

  def test_uses_fallback_duration_for_unknown_tests
    tests = create_mock_tests(['UnknownTest#test_1', 'UnknownTest#test_2'])
    timing_data = {}
    @config.timing_fallback_duration = 500.0

    chunks = order_with_timing(tests, timing_data)

    chunk = chunks.find { |c| c.suite_name == 'UnknownTest' }
    assert_equal 1000.0, chunk.estimated_duration # 2 tests * 500ms
  end

  def test_orders_chunks_by_duration_descending
    tests = create_mock_tests([
      'FastTest#test_1',
      'SlowTest#test_1',
      'MediumTest#test_1'
    ])
    timing_data = {
      'FastTest#test_1' => 1000.0,
      'SlowTest#test_1' => 10_000.0,
      'MediumTest#test_1' => 5000.0
    }

    chunks = order_with_timing(tests, timing_data)

    # Should be ordered: SlowTest, MediumTest, FastTest
    assert_equal 'SlowTest', chunks[0].suite_name
    assert_equal 'MediumTest', chunks[1].suite_name
    assert_equal 'FastTest', chunks[2].suite_name
  end

  def test_handles_empty_test_list
    chunks = @strategy.order_tests([])
    assert_equal [], chunks
  end

  def test_handles_missing_timing_file
    tests = create_mock_tests(['TestA#test_1'])
    @config.timing_file = '/nonexistent/file.json'

    chunks = @strategy.order_tests(tests)

    # Should use fallback duration
    assert_equal 1, chunks.size
    assert_equal 100.0, chunks.first.estimated_duration
  end

  def test_handles_malformed_timing_file
    tests = create_mock_tests(['TestA#test_1'])

    Tempfile.open(['timing', '.json']) do |file|
      file.write('{ invalid json }')
      file.close

      @config.timing_file = file.path
      chunks = @strategy.order_tests(tests)

      # Should use fallback duration
      assert_equal 1, chunks.size
      assert_equal 100.0, chunks.first.estimated_duration
    end
  end

  def test_chunk_ids_are_deterministic
    tests = create_mock_tests(['TestSuite#test_1'])
    chunks1 = @strategy.order_tests(tests)
    chunks2 = @strategy.order_tests(tests)

    assert_equal chunks1.first.id, chunks2.first.id
  end

  def test_split_suite_chunk_ids_include_index
    tests = create_mock_tests([
      'LargeTest#test_1',
      'LargeTest#test_2',
      'LargeTest#test_3'
    ])
    timing_data = {
      'LargeTest#test_1' => 80_000.0,
      'LargeTest#test_2' => 70_000.0,
      'LargeTest#test_3' => 60_000.0
    }

    chunks = order_with_timing(tests, timing_data)
    large_test_chunks = chunks.select { |c| c.suite_name == 'LargeTest' }

    # Check that chunk IDs have indices
    large_test_chunks.each do |chunk|
      assert_match(/LargeTest:chunk_\d+/, chunk.id)
    end
  end

  def test_full_suite_chunk_id_format
    tests = create_mock_tests(['SmallTest#test_1'])
    chunks = @strategy.order_tests(tests)

    chunk = chunks.find { |c| c.suite_name == 'SmallTest' }
    assert_equal 'SmallTest:full_suite', chunk.id
  end

  private

  def create_mock_tests(test_ids)
    test_ids.map do |id|
      MockTest.new(id)
    end
  end

  def order_with_timing(tests, timing_data)
    Tempfile.open(['timing', '.json']) do |file|
      file.write(JSON.generate(timing_data))
      file.close

      @config.timing_file = file.path
      # Recreate strategy to load the new timing data
      strategy = CI::Queue::Strategy::SuiteBinPacking.new(@config)
      strategy.order_tests(tests)
    end
  end

  class MockTest
    attr_reader :id

    def initialize(id)
      @id = id
    end

    def <=>(other)
      id <=> other.id
    end
  end
end
