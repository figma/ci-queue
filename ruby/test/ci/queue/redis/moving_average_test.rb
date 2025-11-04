# frozen_string_literal: true
require 'test_helper'

class CI::Queue::Redis::MovingAverageTest < Minitest::Test
  def setup
    @redis_url = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
    @redis = Redis.new(url: @redis_url)
    @redis.flushdb
    @key = 'test:moving_averages'
  end

  def teardown
    @redis.flushdb
    @redis.close
  end

  def test_initialize
    reader = CI::Queue::Redis::TestDurationMovingAverages.new(@redis, key: @key)
    assert_equal 0, reader.size
  end

  def test_initialize_with_custom_smoothing_factor
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key, smoothing_factor: 0.5)
    reader = CI::Queue::Redis::TestDurationMovingAverages.new(@redis, key: @key)

    updater.update('test1', 100.0)
    first_avg = reader['test1']
    assert_equal 100.0, first_avg

    updater.update('test1', 200.0)
    second_avg = reader['test1']
    assert_in_delta 150.0, second_avg, 0.001
  end

  def test_update_creates_new_entry
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key)
    reader = CI::Queue::Redis::TestDurationMovingAverages.new(@redis, key: @key)

    result = updater.update('test1', 10.5)

    assert_equal 10.5, result
    assert_equal 1, reader.size
  end

  def test_update_calculates_exponential_moving_average
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key, smoothing_factor: 0.2)

    first_avg = updater.update('test1', 100.0)
    assert_equal 100.0, first_avg

    # EMA = 0.2 * 150 + 0.8 * 100 = 30 + 80 = 110
    second_avg = updater.update('test1', 150.0)
    assert_in_delta 110.0, second_avg, 0.001

    third_avg = updater.update('test1', 200.0)
    assert_in_delta 128.0, third_avg, 0.001
  end

  def test_update_multiple_tests
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key)
    reader = CI::Queue::Redis::TestDurationMovingAverages.new(@redis, key: @key)

    updater.update('test1', 10.0)
    updater.update('test2', 20.0)
    updater.update('test3', 30.0)

    assert_equal 3, reader.size
  end

  def test_bracket_operator_loads_and_returns_value
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key)
    updater.update('test1', 10.5)
    updater.update('test2', 20.5)

    reader = CI::Queue::Redis::TestDurationMovingAverages.new(@redis, key: @key)
    assert_equal 10.5, reader['test1']
    assert_equal 20.5, reader['test2']
  end

  def test_bracket_operator_returns_nil_for_missing_key
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key)
    reader = CI::Queue::Redis::TestDurationMovingAverages.new(@redis, key: @key)

    updater.update('test1', 10.5)

    assert_nil reader['nonexistent']
  end

  def test_load_all_loads_all_values_from_redis
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key)
    updater.update('test1', 10.0)
    updater.update('test2', 20.0)
    updater.update('test3', 30.0)

    reader = CI::Queue::Redis::TestDurationMovingAverages.new(@redis, key: @key)
    reader.load_all

    assert_equal 10.0, reader['test1']
    assert_equal 20.0, reader['test2']
    assert_equal 30.0, reader['test3']
  end

  def test_load_all_handles_batches
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key)

    1500.times do |i|
      updater.update("test_#{i}", i.to_f)
    end

    # Create new instance and load all
    reader = CI::Queue::Redis::TestDurationMovingAverages.new(@redis, key: @key)
    reader.load_all

    assert_equal 1500, reader.size
    assert_equal 0.0, reader['test_0']
    assert_equal 500.0, reader['test_500']
    assert_equal 1499.0, reader['test_1499']
  end

  def test_size_returns_correct_count
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key)
    reader = CI::Queue::Redis::TestDurationMovingAverages.new(@redis, key: @key)

    assert_equal 0, reader.size

    updater.update('test1', 10.0)
    assert_equal 1, reader.size

    updater.update('test2', 20.0)
    assert_equal 2, reader.size

    updater.update('test1', 15.0)
    assert_equal 2, reader.size
  end

  def test_updates_persist_to_redis
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key)
    updater.update('test1', 10.5)
    updater.update('test2', 20.5)

    # Verify data is actually in Redis
    values = @redis.hgetall(@key)
    assert_equal 2, values.size
    assert_equal '10.5', values['test1']
    assert_equal '20.5', values['test2']
  end

  def test_concurrent_updates_from_different_instances
    updater1 = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key, smoothing_factor: 0.2)
    updater2 = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key, smoothing_factor: 0.2)
    reader = CI::Queue::Redis::TestDurationMovingAverages.new(@redis, key: @key)

    updater1.update('test1', 100.0)
    updater2.update('test1', 200.0)

    # Expected: 0.2 * 200 + 0.8 * 100 = 120
    assert_in_delta 120.0, reader['test1'], 0.001
  end

  def test_handles_floating_point_precision
    updater = CI::Queue::Redis::UpdateTestDurationMovingAverage.new(@redis, key: @key)

    # Test with various floating point values
    updater.update('test1', 0.123456789)
    updater.update('test2', 999.999999)
    updater.update('test3', 0.000001)

    # Load in new instance
    reader = CI::Queue::Redis::TestDurationMovingAverages.new(@redis, key: @key)

    assert_in_delta 0.123456789, reader['test1'], 0.000001
    assert_in_delta 999.999999, reader['test2'], 0.000001
    assert_in_delta 0.000001, reader['test3'], 0.000001
  end
end
