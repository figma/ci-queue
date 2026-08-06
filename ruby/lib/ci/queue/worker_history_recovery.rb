# frozen_string_literal: true

module CI
  module Queue
    class WorkerHistoryRecovery
      attr_reader :config, :history_items, :replayed_tests

      def initialize(shared_queue, history)
        @shared_queue = shared_queue
        @config = shared_queue.config
        @history_items = history.history_items
        @replay_ids = history.test_ids.dup
        @replayed_tests = history.test_ids.size
        @replay_completed = false
        @resumed_shared_queue = false
        @replay_failures = 0
        @shutdown_required = false
        @phase = :replay
      end

      def distributed?
        true
      end

      def populate(tests, random: Random.new)
        @index = tests.map { |test| [test.id, test] }.to_h
        shared_queue.populate(tests, random: random)
        self
      end

      def populated?
        defined?(@index) && shared_queue.populated?
      end

      def poll(&block)
        while replaying? && replay_allowed? && (id = @replay_ids.shift)
          block.call(index.fetch(id))
        end

        return unless @replay_ids.empty?
        return unless replay_allowed?

        @replay_completed = true
        @resumed_shared_queue = true
        @phase = :shared
        shared_queue.poll(&block)
      end

      def replay_completed?
        @replay_completed
      end

      def resumed_shared_queue?
        @resumed_shared_queue
      end

      def acknowledge(test)
        return shared_queue.acknowledge(test) unless replaying?

        true
      end

      def requeue(test, **options)
        return false if replaying?

        if options.empty?
          shared_queue.requeue(test)
        else
          shared_queue.requeue(test, **options)
        end
      end

      def increment_test_failed
        if replaying?
          @replay_failures += 1
        else
          shared_queue.increment_test_failed
        end
      end

      def test_failed
        replaying? ? @replay_failures : shared_queue.test_failed
      end

      def max_test_failed?
        return false if config.max_test_failed.nil?

        test_failed >= config.max_test_failed
      end

      def exhausted?
        @replay_ids.empty? && shared_queue.exhausted?
      end

      def size
        @replay_ids.size + shared_queue.size
      end

      def total
        shared_queue.total
      end

      def progress
        shared_queue.progress
      end

      def to_a
        @replay_ids.map { |id| index.fetch(id) } + shared_queue.to_a
      end

      def build
        shared_queue.build
      end

      def supervisor
        shared_queue.supervisor
      end

      def retrying?
        true
      end

      def retry_queue
        self
      end

      def expired?
        shared_queue.expired?
      end

      def created_at=(timestamp)
        shared_queue.created_at = timestamp
      end

      def release!
        shared_queue.release!
      end

      def shutdown!
        @shutdown_required = true
        shared_queue.shutdown!
      end

      def flaky?(test)
        shared_queue.flaky?(test)
      end

      def report_failure!
        shared_queue.report_failure!
      end

      def report_success!
        shared_queue.report_success!
      end

      def rescue_connection_errors(handler = ->(_error) { nil }, &block)
        shared_queue.rescue_connection_errors(handler, &block)
      end

      private

      attr_reader :index, :shared_queue

      def replaying?
        @phase == :replay
      end

      def replay_allowed?
        !@shutdown_required && config.circuit_breakers.none?(&:open?) && !max_test_failed?
      end
    end
  end
end
