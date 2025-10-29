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
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)
    assert_equal 0, ma.size
  end

  def test_initialize_with_custom_smoothing_factor
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key, smoothing_factor: 0.5)
    ma.update('test1', 100.0)
    first_avg = ma['test1']
    assert_equal 100.0, first_avg

    ma.update('test1', 200.0)
    second_avg = ma['test1']
    assert_in_delta 150.0, second_avg, 0.001
  end

  def test_update_creates_new_entry
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)
    result = ma.update('test1', 10.5)

    assert_equal 10.5, result
    assert_equal 1, ma.size
  end

  def test_update_calculates_exponential_moving_average
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key, smoothing_factor: 0.2)

    first_avg = ma.update('test1', 100.0)
    assert_equal 100.0, first_avg

    # EMA = 0.2 * 150 + 0.8 * 100 = 30 + 80 = 110
    second_avg = ma.update('test1', 150.0)
    assert_in_delta 110.0, second_avg, 0.001

    third_avg = ma.update('test1', 200.0)
    assert_in_delta 128.0, third_avg, 0.001
  end

  def test_update_multiple_tests
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)

    ma.update('test1', 10.0)
    ma.update('test2', 20.0)
    ma.update('test3', 30.0)

    assert_equal 3, ma.size
  end

  def test_bracket_operator_loads_and_returns_value
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)
    ma.update('test1', 10.5)
    ma.update('test2', 20.5)

    ma2 = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)
    assert_equal 10.5, ma2['test1']
    assert_equal 20.5, ma2['test2']
  end

  def test_bracket_operator_returns_nil_for_missing_key
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)
    ma.update('test1', 10.5)

    assert_nil ma['nonexistent']
  end

  def test_load_all_loads_all_values_from_redis
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)
    ma.update('test1', 10.0)
    ma.update('test2', 20.0)
    ma.update('test3', 30.0)

    ma2 = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)
    ma2.load_all

    assert_equal 10.0, ma2['test1']
    assert_equal 20.0, ma2['test2']
    assert_equal 30.0, ma2['test3']
  end

  def test_load_all_handles_batches
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)

    1500.times do |i|
      ma.update("test_#{i}", i.to_f)
    end

    # Create new instance and load all
    ma2 = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)
    ma2.load_all

    assert_equal 1500, ma2.size
    assert_equal 0.0, ma2['test_0']
    assert_equal 500.0, ma2['test_500']
    assert_equal 1499.0, ma2['test_1499']
  end

  def test_size_returns_correct_count
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)

    assert_equal 0, ma.size

    ma.update('test1', 10.0)
    assert_equal 1, ma.size

    ma.update('test2', 20.0)
    assert_equal 2, ma.size

    ma.update('test1', 15.0)
    assert_equal 2, ma.size
  end

  def test_updates_persist_to_redis
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)
    ma.update('test1', 10.5)
    ma.update('test2', 20.5)

    # Verify data is actually in Redis
    values = @redis.hgetall(@key)
    assert_equal 2, values.size
    assert_equal '10.5', values['test1']
    assert_equal '20.5', values['test2']
  end

  def test_concurrent_updates_from_different_instances
    ma1 = CI::Queue::Redis::MovingAverage.new(@redis, key: @key, smoothing_factor: 0.2)
    ma2 = CI::Queue::Redis::MovingAverage.new(@redis, key: @key, smoothing_factor: 0.2)

    ma1.update('test1', 100.0)
    ma2.update('test1', 200.0)

    # Expected: 0.2 * 200 + 0.8 * 100 = 120
    assert_in_delta 120.0, ma2['test1'], 0.001
  end

  def test_handles_floating_point_precision
    ma = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)

    # Test with various floating point values
    ma.update('test1', 0.123456789)
    ma.update('test2', 999.999999)
    ma.update('test3', 0.000001)

    # Load in new instance
    ma2 = CI::Queue::Redis::MovingAverage.new(@redis, key: @key)

    assert_in_delta 0.123456789, ma2['test1'], 0.000001
    assert_in_delta 999.999999, ma2['test2'], 0.000001
    assert_in_delta 0.000001, ma2['test3'], 0.000001
  end
end
