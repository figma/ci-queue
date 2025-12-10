# frozen_string_literal: true
require 'test_helper'
require 'tempfile'

class SuiteBinPackingTest < Minitest::Test
  def setup
    @config = CI::Queue::Configuration.new(
      minimum_max_chunk_duration: 120_000,
      maximum_max_chunk_duration: 300_000,
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

  def test_creates_single_chunk_when_under_max_duration
    tests = create_mock_tests(['SmallTest#test_1', 'SmallTest#test_2'])
    timing_data = {
      'SmallTest#test_1' => 1000.0,
      'SmallTest#test_2' => 2000.0
    }

    chunks = order_with_timing(tests, timing_data)

    chunk = chunks.find { |c| c.suite_name == 'SmallTest' }
    assert_equal 3000.0, chunk.estimated_duration
    # Tests preserve original order (as they appear in the file)
    assert_equal ['SmallTest#test_1', 'SmallTest#test_2'], chunk.test_ids
    assert_equal 'SmallTest:chunk_0', chunk.id
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
      assert chunk.test_ids.any?, 'Chunk should have test_ids'
      assert_match(/LargeTest:chunk_\d+/, chunk.id)
    end
  end

  def test_applies_buffer_when_splitting
    @config.minimum_max_chunk_duration = 100_000
    @config.maximum_max_chunk_duration = 100_000
    @config.suite_buffer_percent = 10

    tests = create_mock_tests(['TestSuite#test_1', 'TestSuite#test_2'])
    timing_data = {
      'TestSuite#test_1' => 50_000.0,
      'TestSuite#test_2' => 45_000.0
    }

    chunks = order_with_timing(tests, timing_data)

    # Total is 95,000 which is under 100,000 but over 90,000 (with 10% buffer)
    # Buffer is always applied, so effective_max = 90,000
    # Since 95,000 > 90,000, it gets split into 2 chunks
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }
    assert_equal 2, test_suite_chunks.size
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

  def test_chunk_id_format
    tests = create_mock_tests(['SmallTest#test_1'])
    chunks = @strategy.order_tests(tests)

    chunk = chunks.find { |c| c.suite_name == 'SmallTest' }
    assert_equal 'SmallTest:chunk_0', chunk.id
  end

  def test_suite_exactly_at_max_duration_gets_split
    @config.minimum_max_chunk_duration = 100_000
    @config.maximum_max_chunk_duration = 100_000
    @config.suite_buffer_percent = 10

    tests = create_mock_tests(['TestSuite#test_1', 'TestSuite#test_2'])
    timing_data = {
      'TestSuite#test_1' => 50_000.0,
      'TestSuite#test_2' => 50_000.0
    }

    chunks = order_with_timing(tests, timing_data)
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }

    # Total is exactly 100,000, but with 10% buffer, effective_max = 90,000
    # So it should be split
    assert_equal 2, test_suite_chunks.size
  end

  def test_suite_exactly_at_effective_max_fits_in_one_chunk
    @config.minimum_max_chunk_duration = 100_000
    @config.maximum_max_chunk_duration = 100_000
    @config.suite_buffer_percent = 10

    tests = create_mock_tests(['TestSuite#test_1', 'TestSuite#test_2'])
    timing_data = {
      'TestSuite#test_1' => 45_000.0,
      'TestSuite#test_2' => 45_000.0
    }

    chunks = order_with_timing(tests, timing_data)
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }

    # Total is exactly 90,000 (effective_max), should fit in one chunk
    assert_equal 1, test_suite_chunks.size
    assert_equal 90_000.0, test_suite_chunks.first.estimated_duration
  end

  def test_tests_preserve_original_order
    tests = create_mock_tests([
      'TestSuite#test_1',
      'TestSuite#test_2',
      'TestSuite#test_3'
    ])
    timing_data = {
      'TestSuite#test_1' => 1000.0,
      'TestSuite#test_2' => 2000.0,
      'TestSuite#test_3' => 3000.0
    }

    chunks = order_with_timing(tests, timing_data)
    chunk = chunks.find { |c| c.suite_name == 'TestSuite' }

    # Tests should preserve original order (as they appear in the file)
    assert_equal 3, chunk.test_ids.size
    assert_equal ['TestSuite#test_1', 'TestSuite#test_2', 'TestSuite#test_3'], chunk.test_ids
  end

  def test_chunk_duration_calculation_is_correct
    tests = create_mock_tests([
      'TestSuite#test_1',
      'TestSuite#test_2',
      'TestSuite#test_3'
    ])
    timing_data = {
      'TestSuite#test_1' => 10_000.0,
      'TestSuite#test_2' => 20_000.0,
      'TestSuite#test_3' => 30_000.0
    }

    chunks = order_with_timing(tests, timing_data)
    chunk = chunks.find { |c| c.suite_name == 'TestSuite' }

    # Duration should be sum of all test durations
    assert_equal 60_000.0, chunk.estimated_duration
  end

  def test_test_count_is_set_correctly
    tests = create_mock_tests([
      'TestSuite#test_1',
      'TestSuite#test_2',
      'TestSuite#test_3',
      'TestSuite#test_4'
    ])
    timing_data = {
      'TestSuite#test_1' => 10_000.0,
      'TestSuite#test_2' => 10_000.0,
      'TestSuite#test_3' => 10_000.0,
      'TestSuite#test_4' => 10_000.0
    }

    chunks = order_with_timing(tests, timing_data)
    chunk = chunks.find { |c| c.suite_name == 'TestSuite' }

    assert_equal 4, chunk.test_count
    assert_equal 4, chunk.test_ids.size
  end

  def test_large_suite_creates_multiple_chunks
    @config.minimum_max_chunk_duration = 100_000
    @config.maximum_max_chunk_duration = 100_000
    @config.suite_buffer_percent = 10

    # Create a suite that will need many chunks
    tests = create_mock_tests((1..10).map { |i| "LargeSuite#test_#{i}" })
    timing_data = (1..10).each_with_object({}) do |i, hash|
      hash["LargeSuite#test_#{i}"] = 50_000.0 # Each test is 50% of max
    end

    chunks = order_with_timing(tests, timing_data)
    large_suite_chunks = chunks.select { |c| c.suite_name == 'LargeSuite' }

    # With effective_max = 90,000, each test is 50,000, so we can fit 1 per chunk
    # So 10 tests = 10 chunks
    assert_equal 10, large_suite_chunks.size
    large_suite_chunks.each do |chunk|
      assert_equal 1, chunk.test_ids.size
      assert_equal 50_000.0, chunk.estimated_duration
    end
  end

  def test_zero_duration_tests_handled_correctly
    tests = create_mock_tests(['TestSuite#test_1', 'TestSuite#test_2'])
    timing_data = {
      'TestSuite#test_1' => 0.0,
      'TestSuite#test_2' => 1000.0
    }

    chunks = order_with_timing(tests, timing_data)
    chunk = chunks.find { |c| c.suite_name == 'TestSuite' }

    # Should handle zero duration correctly
    assert_equal 1000.0, chunk.estimated_duration
    assert_equal 2, chunk.test_ids.size
  end

  def test_single_test_creates_chunk
    tests = create_mock_tests(['SingleTest#test_1'])
    timing_data = {
      'SingleTest#test_1' => 5000.0
    }

    chunks = order_with_timing(tests, timing_data)
    chunk = chunks.find { |c| c.suite_name == 'SingleTest' }

    assert_equal 1, chunks.size
    assert_equal 'SingleTest:chunk_0', chunk.id
    assert_equal ['SingleTest#test_1'], chunk.test_ids
    assert_equal 5000.0, chunk.estimated_duration
    assert_equal 1, chunk.test_count
  end

  def test_extract_suite_name_with_regular_format
    tests = create_mock_tests([
      'UserTest#test_create',
      'UserTest#test_update',
      'OrderTest#test_process'
    ])

    chunks = @strategy.order_tests(tests)

    suite_names = chunks.map(&:suite_name).uniq.sort
    assert_equal ['OrderTest', 'UserTest'], suite_names
  end

  def test_extract_suite_name_with_spec_style_single_colon
    tests = create_mock_tests([
      'Users::UserTest#test_create',
      'Users::UserTest#test_update',
      'Orders::OrderTest#test_process'
    ])

    chunks = @strategy.order_tests(tests)

    suite_names = chunks.map(&:suite_name).uniq.sort
    # Should extract 'Users' and 'Orders' (first part before ::)
    assert_equal ['Orders', 'Users'], suite_names
  end

  def test_extract_suite_name_with_spec_style_multiple_colons
    tests = create_mock_tests([
      'Api::V1::UsersControllerTest#test_index',
      'Api::V1::UsersControllerTest#test_show',
      'Api::V2::OrdersControllerTest#test_create'
    ])

    chunks = @strategy.order_tests(tests)

    suite_names = chunks.map(&:suite_name).uniq.sort
    # Should extract 'Api' (first part before first ::)
    assert_equal ['Api'], suite_names
    # All tests should be grouped under 'Api'
    api_chunks = chunks.select { |c| c.suite_name == 'Api' }
    assert_equal 1, api_chunks.size
  end

  def test_extract_suite_name_mixed_formats
    tests = create_mock_tests([
      'SimpleTest#test_one',
      'Module::NestedTest#test_two',
      'AnotherSimpleTest#test_three'
    ])

    chunks = @strategy.order_tests(tests)

    suite_names = chunks.map(&:suite_name).uniq.sort
    # SimpleTest and AnotherSimpleTest should remain as-is
    # Module::NestedTest should extract to 'Module'
    assert_equal ['AnotherSimpleTest', 'Module', 'SimpleTest'], suite_names
  end

  def test_extract_suite_name_preserves_grouping_with_spec_style
    tests = create_mock_tests([
      'Api::UsersControllerTest#test_index',
      'Api::UsersControllerTest#test_show',
      'Api::UsersControllerTest#test_create',
      'Web::UsersControllerTest#test_index'
    ])

    chunks = @strategy.order_tests(tests)

    # Api::* tests should be grouped under 'Api'
    api_chunks = chunks.select { |c| c.suite_name == 'Api' }
    assert_equal 1, api_chunks.size
    assert_equal 3, api_chunks.first.test_count

    # Web::* tests should be grouped under 'Web'
    web_chunks = chunks.select { |c| c.suite_name == 'Web' }
    assert_equal 1, web_chunks.size
    assert_equal 1, web_chunks.first.test_count
  end

  def test_extract_suite_name_with_spaces_in_scenario_name
    tests = create_mock_tests([
      'Api::V1::UsersController#test create user',
      'Api::V1::UsersController#test create user with valid data',
      'Api::V1::UsersController#test update user profile',
      'Web::V1::OrdersController#test process order'
    ])

    chunks = @strategy.order_tests(tests)

    suite_names = chunks.map(&:suite_name).uniq.sort
    # Should extract 'Api' and 'Web' (first part before ::)
    # Spaces in scenario names should not affect suite name extraction
    assert_equal ['Api', 'Web'], suite_names

    # Api::* tests should be grouped under 'Api' regardless of spaces in scenario names
    api_chunks = chunks.select { |c| c.suite_name == 'Api' }
    assert_equal 1, api_chunks.size
    assert_equal 3, api_chunks.first.test_count

    # Web::* tests should be grouped under 'Web'
    web_chunks = chunks.select { |c| c.suite_name == 'Web' }
    assert_equal 1, web_chunks.size
    assert_equal 1, web_chunks.first.test_count
  end

  def test_dynamic_max_duration_calculation
    # Set up parallel job count
    ENV['BUILDKITE_PARALLEL_JOB_COUNT'] = '4'
    
    tests = create_mock_tests([
      'TestSuite#test_1',
      'TestSuite#test_2',
      'TestSuite#test_3',
      'TestSuite#test_4'
    ])
    timing_data = {
      'TestSuite#test_1' => 10_000.0,
      'TestSuite#test_2' => 10_000.0,
      'TestSuite#test_3' => 10_000.0,
      'TestSuite#test_4' => 10_000.0
    }

    chunks = order_with_timing(tests, timing_data)
    
    # Total duration = 40,000ms
    # Parallel jobs = 4
    # Calculated base_max_duration = 40,000 / 4 = 10,000ms
    # However, 10,000ms < configured minimum_max_chunk_duration (120,000ms), so floor logic applies
    # Actual max_duration used = max(10,000, 120,000) = 120,000ms
    # With 10% buffer, effective_max = 120,000 * 0.9 = 108,000ms
    # All 4 tests fit in one chunk: 4 * 10,000 = 40,000ms < 108,000ms
    # Due to floor logic, everything fits in one chunk
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }
    assert_equal 1, test_suite_chunks.size, 'Due to floor logic, all tests fit in one chunk'
  ensure
    ENV.delete('BUILDKITE_PARALLEL_JOB_COUNT')
  end

  def test_dynamic_max_duration_falls_back_to_configured_when_no_env_var
    # Ensure env var is not set
    ENV.delete('BUILDKITE_PARALLEL_JOB_COUNT')
    
    tests = create_mock_tests(['TestSuite#test_1'])
    timing_data = { 'TestSuite#test_1' => 1000.0 }

    chunks = order_with_timing(tests, timing_data)
    
    # Should use configured minimum_max_chunk_duration (120,000ms default)
    # So test should fit in one chunk
    assert_equal 1, chunks.size
  end

  def test_dynamic_max_duration_uses_floor_when_calculated_value_too_small
    # Set up parallel job count that would result in very small chunks
    ENV['BUILDKITE_PARALLEL_JOB_COUNT'] = '100'
    
    tests = create_mock_tests(['TestSuite#test_1'])
    timing_data = { 'TestSuite#test_1' => 1000.0 }

    chunks = order_with_timing(tests, timing_data)
    
    # Calculated max = 1000 / 100 = 10ms (too small)
    # Should use configured minimum_max_chunk_duration (120,000ms) as floor
    # So test should fit in one chunk
    assert_equal 1, chunks.size
  ensure
    ENV.delete('BUILDKITE_PARALLEL_JOB_COUNT')
  end

  def test_dynamic_max_duration_with_large_parallelism
    # Set up large parallel job count
    ENV['BUILDKITE_PARALLEL_JOB_COUNT'] = '10'
    
    tests = create_mock_tests((1..20).map { |i| "TestSuite#test_#{i}" })
    timing_data = (1..20).each_with_object({}) do |i, hash|
      hash["TestSuite#test_#{i}"] = 5000.0
    end

    chunks = order_with_timing(tests, timing_data)
    
    # Total duration = 20 * 5000 = 100,000ms
    # Parallel jobs = 10
    # Calculated base_max_duration = 100,000 / 10 = 10,000ms
    # However, 10,000ms < configured minimum_max_chunk_duration (120,000ms), so floor logic applies
    # Actual max_duration used = max(10,000, 120,000) = 120,000ms
    # With 10% buffer, effective_max = 120,000 * 0.9 = 108,000ms
    # All 20 tests fit in one chunk: 20 * 5,000 = 100,000ms < 108,000ms
    # Due to floor logic, everything fits in one chunk
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }
    assert_equal 1, test_suite_chunks.size, 'Due to floor logic, all tests fit in one chunk'
  ensure
    ENV.delete('BUILDKITE_PARALLEL_JOB_COUNT')
  end

  def test_dynamic_max_duration_exceeds_default_max_causes_splits
    # Set up scenario where computed chunk size > default minimum_max_chunk_duration
    ENV['BUILDKITE_PARALLEL_JOB_COUNT'] = '2'
    
    # Create tests with large total duration
    tests = create_mock_tests((1..10).map { |i| "TestSuite#test_#{i}" })
    timing_data = (1..10).each_with_object({}) do |i, hash|
      hash["TestSuite#test_#{i}"] = 30_000.0  # Each test is 30 seconds
    end

    chunks = order_with_timing(tests, timing_data)
    
    # Total duration = 10 * 30,000 = 300,000ms
    # Parallel jobs = 2
    # Calculated base_max_duration = 300,000 / 2 = 150,000ms
    # 150,000ms > configured minimum_max_chunk_duration (120,000ms), so use calculated value
    # Actual max_duration used = max(150,000, 120,000) = 150,000ms
    # With 10% buffer, effective_max = 150,000 * 0.9 = 135,000ms
    # Each test is 30,000ms, so we can fit 4 per chunk (4 * 30,000 = 120,000 < 135,000)
    # But we have 10 tests, so we'll get multiple chunks
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }
    assert test_suite_chunks.size >= 2, 'Should create multiple chunks when computed max exceeds default'
    
    # Verify chunks use the larger calculated max_duration
    # First chunk should have ~4 tests (4 * 30,000 = 120,000 < 135,000)
    first_chunk = test_suite_chunks.first
    assert first_chunk.estimated_duration <= 135_000, 'Chunk should respect effective_max'
  ensure
    ENV.delete('BUILDKITE_PARALLEL_JOB_COUNT')
  end

  def test_dynamic_max_duration_capped_at_maximum
    # Set up scenario where computed chunk size would exceed maximum_max_chunk_duration
    ENV['BUILDKITE_PARALLEL_JOB_COUNT'] = '1'
    
    # Create tests with very large total duration
    tests = create_mock_tests((1..10).map { |i| "TestSuite#test_#{i}" })
    timing_data = (1..10).each_with_object({}) do |i, hash|
      hash["TestSuite#test_#{i}"] = 80_000.0  # Each test is 80 seconds
    end

    chunks = order_with_timing(tests, timing_data)
    
    # Total duration = 10 * 80,000 = 800,000ms
    # Parallel jobs = 1
    # Calculated base_max_duration = 800,000 / 1 = 800,000ms
    # 800,000ms > configured maximum_max_chunk_duration (300,000ms), so cap it
    # Actual max_duration used = min(800,000, 300,000) = 300,000ms
    # With 10% buffer, effective_max = 300,000 * 0.9 = 270,000ms
    # Each test is 80,000ms, so we can fit 3 per chunk (3 * 80,000 = 240,000 < 270,000)
    # With 10 tests, we'll get 4 chunks (3+3+3+1)
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }
    assert_equal 4, test_suite_chunks.size, 'Should cap at maximum_max_chunk_duration'
    
    # Verify chunks respect the capped max_duration
    test_suite_chunks[0..2].each do |chunk|
      assert_equal 3, chunk.test_ids.size, 'First 3 chunks should have 3 tests each'
      assert_equal 240_000.0, chunk.estimated_duration
    end
    assert_equal 1, test_suite_chunks[3].test_ids.size, 'Last chunk should have 1 test'
    assert_equal 80_000.0, test_suite_chunks[3].estimated_duration
  ensure
    ENV.delete('BUILDKITE_PARALLEL_JOB_COUNT')
  end

  def test_dynamic_max_duration_within_range
    # Set up scenario where computed chunk size falls within the configured range
    ENV['BUILDKITE_PARALLEL_JOB_COUNT'] = '5'
    
    tests = create_mock_tests((1..10).map { |i| "TestSuite#test_#{i}" })
    timing_data = (1..10).each_with_object({}) do |i, hash|
      hash["TestSuite#test_#{i}"] = 20_000.0  # Each test is 20 seconds
    end

    chunks = order_with_timing(tests, timing_data)
    
    # Total duration = 10 * 20,000 = 200,000ms
    # Parallel jobs = 5
    # Calculated base_max_duration = 200,000 / 5 = 40,000ms
    # 40,000ms < minimum_max_chunk_duration (120,000ms), so use minimum
    # Actual max_duration used = max(40,000, 120,000) = 120,000ms
    # With 10% buffer, effective_max = 120,000 * 0.9 = 108,000ms
    # Each test is 20,000ms, so we can fit 5 per chunk (5 * 20,000 = 100,000 < 108,000)
    # With 10 tests, we'll get 2 chunks (5+5)
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }
    assert_equal 2, test_suite_chunks.size, 'Should create 2 chunks with minimum floor applied'
    
    test_suite_chunks.each do |chunk|
      assert_equal 5, chunk.test_ids.size, 'Each chunk should have 5 tests'
      assert_equal 100_000.0, chunk.estimated_duration
    end
  ensure
    ENV.delete('BUILDKITE_PARALLEL_JOB_COUNT')
  end

  def test_range_based_max_duration_with_custom_values
    # Test with custom minimum and maximum values
    @config.minimum_max_chunk_duration = 60_000
    @config.maximum_max_chunk_duration = 180_000
    
    ENV['BUILDKITE_PARALLEL_JOB_COUNT'] = '2'
    
    tests = create_mock_tests((1..10).map { |i| "TestSuite#test_#{i}" })
    timing_data = (1..10).each_with_object({}) do |i, hash|
      hash["TestSuite#test_#{i}"] = 20_000.0  # Each test is 20 seconds
    end

    chunks = order_with_timing(tests, timing_data)
    
    # Total duration = 10 * 20,000 = 200,000ms
    # Parallel jobs = 2
    # Calculated base_max_duration = 200,000 / 2 = 100,000ms
    # 100,000ms is within range [60,000, 180,000], so use it
    # With 10% buffer, effective_max = 100,000 * 0.9 = 90,000ms
    # Each test is 20,000ms, so we can fit 4 per chunk (4 * 20,000 = 80,000 < 90,000)
    # With 10 tests, we'll get 3 chunks (4+4+2)
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }
    assert_equal 3, test_suite_chunks.size, 'Should create 3 chunks with calculated max_duration'
    
    assert_equal 4, test_suite_chunks[0].test_ids.size, 'First chunk should have 4 tests'
    assert_equal 80_000.0, test_suite_chunks[0].estimated_duration
    assert_equal 4, test_suite_chunks[1].test_ids.size, 'Second chunk should have 4 tests'
    assert_equal 80_000.0, test_suite_chunks[1].estimated_duration
    assert_equal 2, test_suite_chunks[2].test_ids.size, 'Third chunk should have 2 tests'
    assert_equal 40_000.0, test_suite_chunks[2].estimated_duration
  ensure
    ENV.delete('BUILDKITE_PARALLEL_JOB_COUNT')
  end

  def test_maximum_max_duration_prevents_oversized_chunks
    # Test that maximum prevents chunks from being too large
    @config.minimum_max_chunk_duration = 50_000
    @config.maximum_max_chunk_duration = 100_000
    
    ENV['BUILDKITE_PARALLEL_JOB_COUNT'] = '1'
    
    tests = create_mock_tests((1..5).map { |i| "TestSuite#test_#{i}" })
    timing_data = (1..5).each_with_object({}) do |i, hash|
      hash["TestSuite#test_#{i}"] = 40_000.0  # Each test is 40 seconds
    end

    chunks = order_with_timing(tests, timing_data)
    
    # Total duration = 5 * 40,000 = 200,000ms
    # Parallel jobs = 1
    # Calculated base_max_duration = 200,000 / 1 = 200,000ms
    # 200,000ms > maximum_max_chunk_duration (100,000ms), so cap it
    # Actual max_duration used = min(200,000, 100,000) = 100,000ms
    # With 10% buffer, effective_max = 100,000 * 0.9 = 90,000ms
    # Each test is 40,000ms, so we can fit 2 per chunk (2 * 40,000 = 80,000 < 90,000)
    # With 5 tests, we'll get 3 chunks (2+2+1)
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }
    assert_equal 3, test_suite_chunks.size, 'Should cap at maximum and create smaller chunks'
    
    assert_equal 2, test_suite_chunks[0].test_ids.size
    assert_equal 2, test_suite_chunks[1].test_ids.size
    assert_equal 1, test_suite_chunks[2].test_ids.size
  ensure
    ENV.delete('BUILDKITE_PARALLEL_JOB_COUNT')
  end

  def test_minimum_max_duration_prevents_undersized_chunks
    # Test that minimum prevents chunks from being too small
    @config.minimum_max_chunk_duration = 200_000
    @config.maximum_max_chunk_duration = 500_000
    
    ENV['BUILDKITE_PARALLEL_JOB_COUNT'] = '20'
    
    tests = create_mock_tests((1..10).map { |i| "TestSuite#test_#{i}" })
    timing_data = (1..10).each_with_object({}) do |i, hash|
      hash["TestSuite#test_#{i}"] = 20_000.0  # Each test is 20 seconds
    end

    chunks = order_with_timing(tests, timing_data)
    
    # Total duration = 10 * 20,000 = 200,000ms
    # Parallel jobs = 20
    # Calculated base_max_duration = 200,000 / 20 = 10,000ms
    # 10,000ms < minimum_max_chunk_duration (200,000ms), so use minimum
    # Actual max_duration used = max(10,000, 200,000) = 200,000ms
    # With 10% buffer, effective_max = 200,000 * 0.9 = 180,000ms
    # Each test is 20,000ms, so we can fit 9 per chunk (9 * 20,000 = 180,000 = 180,000)
    # With 10 tests, we'll get 2 chunks (9+1)
    test_suite_chunks = chunks.select { |c| c.suite_name == 'TestSuite' }
    assert_equal 2, test_suite_chunks.size, 'Should use minimum and create larger chunks'
    
    assert_equal 9, test_suite_chunks[0].test_ids.size, 'First chunk should have 9 tests'
    assert_equal 180_000.0, test_suite_chunks[0].estimated_duration
    assert_equal 1, test_suite_chunks[1].test_ids.size, 'Second chunk should have 1 test'
    assert_equal 20_000.0, test_suite_chunks[1].estimated_duration
  ensure
    ENV.delete('BUILDKITE_PARALLEL_JOB_COUNT')
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
