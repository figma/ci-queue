# frozen_string_literal: true
module CI
  module Queue
    module Redis
      class Retry < Static
        def initialize(tests, config, redis:)
          @redis = redis
          @poll_completed = false
          super(tests, config)
        end

        def poll_completed?
          @poll_completed
        end

        def poll
          super
          @poll_completed = exhausted?
        end

        def build
          @build ||= CI::Queue::Redis::BuildRecord.new(self, redis, config)
        end

        private

        attr_reader :redis
      end
    end
  end
end
