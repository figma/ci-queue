# frozen_string_literal: true
module CI
  module Queue
    module Redis
      class Retry < Static
        def initialize(tests, config, redis:)
          @redis = redis
          super(tests, config)
        end

        def build
          @build ||= CI::Queue::Redis::BuildRecord.new(self, redis, config)
        end

        def poll
          super
          return unless config.retry_mode == :worker_history
          return if exhausted?

          raise IncompleteRetry, 'Worker history replay stopped before completion'
        end

        private

        attr_reader :redis
      end
    end
  end
end
