# frozen_string_literal: true

require 'fileutils'
require 'json'
require 'minitest/reporters'

module Minitest
  module Queue
    class RecoveryReporter < Minitest::Reporters::BaseReporter
      def initialize(path:, queue:, config:)
        super({})
        @path = path
        @queue = queue
        @config = config
      end

      def report
        super
        return unless queue.exhausted?
        return if recovery? && !queue.replay_completed?

        FileUtils.mkdir_p(File.dirname(path))
        File.write(path, JSON.pretty_generate(manifest))
      end

      private

      attr_reader :config, :path, :queue

      def recovery?
        queue.is_a?(CI::Queue::WorkerHistoryRecovery)
      end

      def manifest
        {
          schema_version: 1,
          worker_id: config.worker_id.to_s,
          retry_count: config.retry_count,
          history_items: recovery? ? queue.history_items : 0,
          replayed_tests: recovery? ? queue.replayed_tests : 0,
          resumed_shared_queue: recovery? && queue.resumed_shared_queue?,
          replay_completed: !recovery? || queue.replay_completed?
        }
      end
    end
  end
end
