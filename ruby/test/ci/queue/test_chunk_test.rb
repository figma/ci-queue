# frozen_string_literal: true
require 'test_helper'

module CI::Queue
  class TestChunkTest < Minitest::Test
    def test_full_suite_chunk_initialization
      chunk = TestChunk.new('UserTest:full_suite', 'UserTest', :full_suite, [], 5000.0)

      assert_equal 'UserTest:full_suite', chunk.id
      assert_equal 'UserTest', chunk.suite_name
      assert_equal :full_suite, chunk.type
      assert_equal [], chunk.test_ids
      assert_equal 5000.0, chunk.estimated_duration
      assert chunk.full_suite?
      refute chunk.partial_suite?
    end

    def test_partial_suite_chunk_initialization
      test_ids = ['UserTest#test_create', 'UserTest#test_update']
      chunk = TestChunk.new('UserTest:chunk_0', 'UserTest', :partial_suite, test_ids, 3000.0)

      assert_equal 'UserTest:chunk_0', chunk.id
      assert_equal 'UserTest', chunk.suite_name
      assert_equal :partial_suite, chunk.type
      assert_equal test_ids, chunk.test_ids
      assert_equal 3000.0, chunk.estimated_duration
      refute chunk.full_suite?
      assert chunk.partial_suite?
    end

    def test_chunk_comparison
      chunk1 = TestChunk.new('TestA:full_suite', 'TestA', :full_suite, [], 5000.0)
      chunk2 = TestChunk.new('TestB:full_suite', 'TestB', :full_suite, [], 3000.0)

      assert chunk1 > chunk2
      assert chunk2 < chunk1
    end

    def test_full_suite_json_serialization
      chunk = TestChunk.new('UserTest:full_suite', 'UserTest', :full_suite, [], 5000.0)
      json = chunk.to_json
      data = JSON.parse(json)

      assert_equal 'full_suite', data['type']
      assert_equal 'UserTest', data['suite_name']
      assert_equal 5000.0, data['estimated_duration']
      refute data.key?('test_ids'), 'full_suite should not include test_ids'
    end

    def test_partial_suite_json_serialization
      test_ids = ['UserTest#test_create', 'UserTest#test_update']
      chunk = TestChunk.new('UserTest:chunk_0', 'UserTest', :partial_suite, test_ids, 3000.0)
      json = chunk.to_json
      data = JSON.parse(json)

      assert_equal 'partial_suite', data['type']
      assert_equal 'UserTest', data['suite_name']
      assert_equal 3000.0, data['estimated_duration']
      assert_equal test_ids, data['test_ids']
    end

    def test_full_suite_json_deserialization
      json = JSON.generate({
        type: 'full_suite',
        suite_name: 'UserTest',
        estimated_duration: 5000.0
      })

      chunk = TestChunk.from_json('UserTest:full_suite', json)

      assert_equal 'UserTest:full_suite', chunk.id
      assert_equal 'UserTest', chunk.suite_name
      assert_equal :full_suite, chunk.type
      assert_equal [], chunk.test_ids
      assert_equal 5000.0, chunk.estimated_duration
    end

    def test_partial_suite_json_deserialization
      test_ids = ['UserTest#test_create', 'UserTest#test_update']
      json = JSON.generate({
        type: 'partial_suite',
        suite_name: 'UserTest',
        estimated_duration: 3000.0,
        test_ids: test_ids
      })

      chunk = TestChunk.from_json('UserTest:chunk_0', json)

      assert_equal 'UserTest:chunk_0', chunk.id
      assert_equal 'UserTest', chunk.suite_name
      assert_equal :partial_suite, chunk.type
      assert_equal test_ids, chunk.test_ids
      assert_equal 3000.0, chunk.estimated_duration
    end

    def test_test_ids_are_frozen
      test_ids = ['UserTest#test_create']
      chunk = TestChunk.new('UserTest:chunk_0', 'UserTest', :partial_suite, test_ids, 1000.0)

      assert chunk.test_ids.frozen?
    end
  end
end
