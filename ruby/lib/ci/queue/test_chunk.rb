# frozen_string_literal: true

module CI
  module Queue
    class TestChunk
      include Comparable

      attr_reader :id, :suite_name, :type, :test_ids, :estimated_duration, :test_count

      def initialize(id, suite_name, type, test_ids, estimated_duration = 0, test_count: nil)
        @id = id
        @suite_name = suite_name
        @type = type
        @test_ids = test_ids.freeze
        @estimated_duration = estimated_duration
        @test_count = test_count || test_ids.size
      end

      def full_suite?
        type == :full_suite
      end

      def partial_suite?
        type == :partial_suite
      end

      # For sorting
      def <=>(other)
        if other.respond_to?(:estimated_duration)
          estimated_duration <=> other.estimated_duration
        else
          0
        end
      end

      # Serialize for Redis storage
      def to_json(*args)
        data = {
          type: type.to_s,
          suite_name: suite_name,
          estimated_duration: estimated_duration,
          test_count: test_count
        }

        # Only include test_ids for partial suites
        data[:test_ids] = test_ids if partial_suite?

        data.to_json(*args)
      end

      # Deserialize from Redis
      def self.from_json(chunk_id, json_string)
        data = JSON.parse(json_string)
        new(
          chunk_id,
          data['suite_name'],
          data['type'].to_sym,
          data['test_ids'] || [], # Empty for full_suite
          data['estimated_duration'],
          test_count: data['test_count']
        )
      end
    end
  end
end
