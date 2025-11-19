# frozen_string_literal: true
require 'test_helper'

module CI::Queue
  class TestChunkTest < Minitest::Test
    def test_chunk_initialization
      test_ids = ['UserTest#test_1', 'UserTest#test_2']
      chunk = TestChunk.new('UserTest:chunk_0', 'UserTest', test_ids, 5000.0)

      assert_equal 'UserTest:chunk_0', chunk.id
      assert_equal 'UserTest', chunk.suite_name
      assert_equal test_ids, chunk.test_ids
      assert_equal 5000.0, chunk.estimated_duration
    end

    def test_chunk_comparison
      chunk1 = TestChunk.new('TestA:chunk_0', 'TestA', ['TestA#test_1'], 5000.0)
      chunk2 = TestChunk.new('TestB:chunk_0', 'TestB', ['TestB#test_1'], 3000.0)

      assert chunk1 > chunk2
      assert chunk2 < chunk1
    end

    def test_json_serialization
      test_ids = ['UserTest#test_1', 'UserTest#test_2']
      chunk = TestChunk.new('UserTest:chunk_0', 'UserTest', test_ids, 5000.0)
      json = chunk.to_json
      data = JSON.parse(json)

      assert_equal 'UserTest', data['suite_name']
      assert_equal 5000.0, data['estimated_duration']
      assert_equal test_ids, data['test_ids']
      assert_equal 2, data['test_count']
    end

    def test_json_deserialization
      test_ids = ['UserTest#test_1', 'UserTest#test_2']
      json = JSON.generate({
        suite_name: 'UserTest',
        estimated_duration: 5000.0,
        test_ids: test_ids,
        test_count: 2
      })

      chunk = TestChunk.from_json('UserTest:chunk_0', json)

      assert_equal 'UserTest:chunk_0', chunk.id
      assert_equal 'UserTest', chunk.suite_name
      assert_equal test_ids, chunk.test_ids
      assert_equal 5000.0, chunk.estimated_duration
    end

    def test_test_ids_are_frozen
      test_ids = ['UserTest#test_create']
      chunk = TestChunk.new('UserTest:chunk_0', 'UserTest', test_ids, 1000.0)

      assert chunk.test_ids.frozen?
    end
  end
end
