# frozen_string_literal: true

require 'test_helper'

module Minitest
  module Queue
    class RecoveryReporterTest < Minitest::Test
      def test_does_not_write_manifest_for_incomplete_run
        Dir.mktmpdir do |directory|
          path = File.join(directory, 'recovery.json')
          queue = Struct.new(:exhausted?).new(false)
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
