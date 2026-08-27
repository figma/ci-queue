# frozen_string_literal: true
require 'test_helper'

module CI::Queue::Redis
  class WorkerIdleExitTest < Minitest::Test
    REDIS_URL = 'redis://localhost:6379/0'

    def test_worker_that_drew_requeue_duty_never_exits_on_idle
      worker = build_worker(idle_exit_probability: 0.0, idle_exit_grace: 0)
      assert worker.waits_for_requeues?

      worker.idle_since = CI::Queue.time_now - 3600
      refute worker.idle_exit?
    end

    def test_worker_without_requeue_duty_waits_out_the_grace_period
      worker = build_worker(idle_exit_probability: 1.0, idle_exit_grace: 30)
      refute worker.waits_for_requeues?

      worker.idle_since = CI::Queue.time_now - 10
      refute worker.idle_exit?

      worker.idle_since = CI::Queue.time_now - 31
      assert worker.idle_exit?
    end

    def test_a_busy_worker_never_exits_on_idle
      worker = build_worker(idle_exit_probability: 1.0, idle_exit_grace: 0)
      assert_nil worker.idle_since
      refute worker.idle_exit?
    end

    def test_slack_duration_is_unknown_until_a_test_finishes
      assert_nil build_worker.slack_duration
    end

    def test_slack_duration_measures_from_the_last_finished_test
      worker = build_worker
      worker.instance_variable_set(:@last_test_finished_at, CI::Queue.time_now - 12)
      assert_in_delta 12, worker.slack_duration, 1
    end

    private

    def build_worker(**options)
      Worker.new(REDIS_URL, CI::Queue::Configuration.new(**options))
    end
  end
end
