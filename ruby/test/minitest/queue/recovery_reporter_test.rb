# frozen_string_literal: true

require 'test_helper'

module Minitest
  module Queue
    class RecoveryReporterTest < Minitest::Test
      Queue = Struct.new(
        :exhausted?,
        :worker_history?,
        :worker_history_complete?,
        :history_items,
        :replayed_tests
      )

      def test_writes_manifest_for_completed_worker_history_retry
        Dir.mktmpdir do |directory|
          path = File.join(directory, 'recovery.json')
          queue = Queue.new(true, true, true, 4, 3)
          config = CI::Queue::Configuration.new(worker_id: '17', retry_count: 2)
          reporter = RecoveryReporter.new(path: path, queue: queue, config: config)

          reporter.start
          reporter.report

          manifest = JSON.parse(File.read(path))
          assert_equal 4, manifest['history_items']
          assert_equal 3, manifest['replayed_tests']
          assert manifest['replay_completed']
          refute manifest['resumed_shared_queue']
        end
      end

      def test_does_not_write_manifest_for_incomplete_retry
        Dir.mktmpdir do |directory|
          path = File.join(directory, 'recovery.json')
          queue = Queue.new(false, true, false, 4, 3)
          config = CI::Queue::Configuration.new(worker_id: '17')
          reporter = RecoveryReporter.new(path: path, queue: queue, config: config)

          reporter.start
          reporter.report

          refute_path_exists path
        end
      end

      def test_does_not_write_manifest_when_replay_did_not_complete
        Dir.mktmpdir do |directory|
          path = File.join(directory, 'recovery.json')
          queue = Queue.new(true, true, false, 4, 3)
          config = CI::Queue::Configuration.new(worker_id: '17')
          reporter = RecoveryReporter.new(path: path, queue: queue, config: config)

          reporter.start
          reporter.report

          refute_path_exists path
        end
      end
    end
  end
end
