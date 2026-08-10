# frozen_string_literal: true

require 'fileutils'
require 'json'
require 'minitest/reporters'
require 'tempfile'

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
        return if worker_history_retry? && !queue.worker_history_complete?

        directory = File.dirname(path)
        FileUtils.mkdir_p(directory)
        temporary = Tempfile.new(['recovery', '.json'], directory)
        temporary.write(JSON.pretty_generate(manifest))
        temporary.flush
        temporary.fsync
        temporary.close
        File.rename(temporary.path, path)
      ensure
        temporary&.close!
      end

      private

      attr_reader :config, :path, :queue

      def worker_history_retry?
        queue.respond_to?(:worker_history?) && queue.worker_history?
      end

      def manifest
        {
          schema_version: 1,
          worker_id: config.worker_id.to_s,
          retry_count: config.retry_count,
          history_items: worker_history_retry? ? queue.history_items : 0,
          replayed_tests: worker_history_retry? ? queue.replayed_tests : 0,
          resumed_shared_queue: false,
          replay_completed: !worker_history_retry? || queue.worker_history_complete?
        }
      end
    end
  end
end
