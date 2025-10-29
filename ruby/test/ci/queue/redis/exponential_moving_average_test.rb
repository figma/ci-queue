# frozen_string_literal: true
require 'test_helper'
require 'ci/queue/redis/exponential_moving_average'

module CI::Queue::Redis
  class ExponentialMovingAverageTest < Minitest::Test
    def setup
      @redis = MockRedis.new
      @ema = ExponentialMovingAverage.new(@redis, hash_key: 'test_timing', alpha: 0.2)
    end

    def test_initialize_with_defaults
      ema = ExponentialMovingAverage.new(@redis)
      assert_equal @redis, ema.redis
      assert_equal 'timing_data', ema.hash_key
      assert_equal 0.2, ema.alpha
    end

    def test_initialize_with_custom_values
      ema = ExponentialMovingAverage.new(@redis, hash_key: 'custom_key', alpha: 0.3)
      assert_equal 'custom_key', ema.hash_key
      assert_equal 0.3, ema.alpha
    end

    def test_update_first_value
      # First update should store the raw value
      @redis.expect_eval = lambda do |script, keys:, argv:|
        assert_includes script, 'HGET'
        assert_includes script, 'HSET'
        assert_equal ['test_timing'], keys
        assert_equal ['Test#method', 1000.0, 0.2], argv
        1000.0 # Return the value
      end

      result = @ema.update('Test#method', 1000.0)
      assert_equal 1000.0, result
    end

    def test_update_calculates_ema
      # Simulate existing value in Redis
      @redis.hset('test_timing', 'Test#method', '1000.0')

      # Second update should calculate EMA: 0.2 * 1200 + 0.8 * 1000 = 1040
      result = @ema.update('Test#method', 1200.0)

      # Verify the EMA calculation (with floating point tolerance)
      expected = 0.2 * 1200.0 + 0.8 * 1000.0
      assert_in_delta expected, result.to_f, 0.01
    end

    def test_load_all_empty_hash
      timing_data = @ema.load_all
      assert_equal({}, timing_data)
    end

    def test_load_all_with_data
      # Setup test data
      @redis.hset('test_timing', 'Test1#method1', '1234.5')
      @redis.hset('test_timing', 'Test2#method2', '2345.6')
      @redis.hset('test_timing', 'Test3#method3', '3456.7')

      timing_data = @ema.load_all

      assert_equal 3, timing_data.size
      assert_equal 1234.5, timing_data['Test1#method1']
      assert_equal 2345.6, timing_data['Test2#method2']
      assert_equal 3456.7, timing_data['Test3#method3']
    end

    def test_load_all_with_custom_count
      # Setup many entries
      100.times do |i|
        @redis.hset('test_timing', "Test#{i}#method", "#{i * 100}.0")
      end

      timing_data = @ema.load_all(count: 10)

      assert_equal 100, timing_data.size
      # Verify values are correctly parsed as floats
      assert_equal 0.0, timing_data['Test0#method']
      assert_equal 9900.0, timing_data['Test99#method']
    end

    def test_import_empty_hash
      @ema.import({})
      assert_equal 0, @redis.hlen('test_timing')
    end

    def test_import_nil
      @ema.import(nil)
      assert_equal 0, @redis.hlen('test_timing')
    end

    def test_import_bulk_data
      timing_data = {
        'Test1#method1' => 1111.1,
        'Test2#method2' => 2222.2,
        'Test3#method3' => 3333.3
      }

      @ema.import(timing_data)

      assert_equal 3, @redis.hlen('test_timing')
      assert_equal '1111.1', @redis.hget('test_timing', 'Test1#method1')
      assert_equal '2222.2', @redis.hget('test_timing', 'Test2#method2')
      assert_equal '3333.3', @redis.hget('test_timing', 'Test3#method3')
    end

    def test_export_all
      @redis.hset('test_timing', 'Test1#method', '1234.5')
      @redis.hset('test_timing', 'Test2#method', '2345.6')

      result = @ema.export_all

      assert_equal 2, result.size
      assert_equal 1234.5, result['Test1#method']
      assert_equal 2345.6, result['Test2#method']
    end

    def test_exists_when_empty
      refute @ema.exists?
    end

    def test_exists_when_has_data
      @redis.hset('test_timing', 'Test#method', '1000.0')
      assert @ema.exists?
    end

    def test_size_when_empty
      assert_equal 0, @ema.size
    end

    def test_size_with_data
      @redis.hset('test_timing', 'Test1#method', '1000.0')
      @redis.hset('test_timing', 'Test2#method', '2000.0')
      @redis.hset('test_timing', 'Test3#method', '3000.0')

      assert_equal 3, @ema.size
    end

    def test_ema_calculation_sequence
      # Test a sequence of updates to verify EMA algorithm
      test_id = 'Test#method'

      # First value: 1000ms
      result1 = @ema.update(test_id, 1000.0)
      assert_equal 1000.0, result1.to_f

      # Second value: 1200ms
      # EMA = 0.2 * 1200 + 0.8 * 1000 = 1040
      result2 = @ema.update(test_id, 1200.0)
      assert_in_delta 1040.0, result2.to_f, 0.01

      # Third value: 1100ms
      # EMA = 0.2 * 1100 + 0.8 * 1040 = 1052
      result3 = @ema.update(test_id, 1100.0)
      assert_in_delta 1052.0, result3.to_f, 0.01

      # Fourth value: 1150ms
      # EMA = 0.2 * 1150 + 0.8 * 1052 = 1071.6
      result4 = @ema.update(test_id, 1150.0)
      assert_in_delta 1071.6, result4.to_f, 0.01
    end

    def test_multiple_tests_independent_ema
      # Verify that different tests have independent EMA calculations
      @ema.update('Test1#method', 1000.0)
      @ema.update('Test2#method', 2000.0)

      @ema.update('Test1#method', 1200.0)
      @ema.update('Test2#method', 2400.0)

      data = @ema.load_all

      # Test1 EMA: 0.2 * 1200 + 0.8 * 1000 = 1040
      assert_in_delta 1040.0, data['Test1#method'], 0.01

      # Test2 EMA: 0.2 * 2400 + 0.8 * 2000 = 2080
      assert_in_delta 2080.0, data['Test2#method'], 0.01
    end
  end
end

# Simple mock Redis class for testing
class MockRedis
  attr_accessor :expect_eval

  def initialize
    @data = {}
  end

  def hset(key, field, value)
    @data[key] ||= {}
    @data[key][field] = value.to_s
  end

  def hget(key, field)
    @data.dig(key, field)
  end

  def hgetall(key)
    @data[key] || {}
  end

  def hscan(key, cursor, count:)
    hash = @data[key] || {}
    entries = hash.to_a.flatten

    # Simple mock: return all data on first call
    if cursor == '0' && !entries.empty?
      ['0', entries]
    else
      ['0', []]
    end
  end

  def hmset(key, *args)
    @data[key] ||= {}
    args.each_slice(2) do |field, value|
      @data[key][field] = value.to_s
    end
  end

  def exists?(key)
    @data.key?(key) && !@data[key].empty?
  end

  def hlen(key)
    @data.dig(key)&.size || 0
  end

  def eval(script, keys:, argv:)
    if @expect_eval
      @expect_eval.call(script, keys: keys, argv: argv)
    else
      # Default EMA implementation for testing
      hash_key = keys[0]
      test_id = argv[0]
      new_duration = argv[1].to_f
      alpha = argv[2].to_f

      current = hget(hash_key, test_id)
      if current
        new_avg = alpha * new_duration + (1 - alpha) * current.to_f
        hset(hash_key, test_id, new_avg)
        new_avg
      else
        hset(hash_key, test_id, new_duration)
        new_duration
      end
    end
  end

  def connected?
    true
  end
end
