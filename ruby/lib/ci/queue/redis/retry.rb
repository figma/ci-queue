# frozen_string_literal: true
module CI
  module Queue
    module Redis
      class Retry < Static
        def initialize(tests, config, redis:, require_exhaustion: false)
          @redis = redis
          @require_exhaustion = require_exhaustion
          super(tests, config)
        end

        def build
          @build ||= CI::Queue::Redis::BuildRecord.new(self, redis, config)
        end

        def poll
          super
          return unless require_exhaustion
          return if exhausted?

          raise IncompleteRetry, 'Worker history replay stopped before completion'
        end

        private

        attr_reader :redis, :require_exhaustion
      end
    end
  end
end
