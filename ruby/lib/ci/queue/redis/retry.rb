# frozen_string_literal: true
module CI
  module Queue
    module Redis
      class Retry < Static
        attr_reader :history_items, :replayed_tests

        def initialize(tests, config, redis:, history_items: 0, worker_history: false)
          @redis = redis
          @history_items = history_items
          @replayed_tests = worker_history ? tests.size : 0
          @worker_history = worker_history
          @worker_history_complete = false
          super(tests, config)
        end

        def worker_history?
          @worker_history
        end

        def worker_history_complete?
          @worker_history_complete
        end

        def poll(&block)
          super
          @worker_history_complete = exhausted? if worker_history?
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
