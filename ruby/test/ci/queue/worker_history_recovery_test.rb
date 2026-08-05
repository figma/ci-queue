# frozen_string_literal: true

require 'test_helper'

class CI::Queue::WorkerHistoryRecoveryTest < Minitest::Test
  Test = Struct.new(:id)
  History = Struct.new(:history_items, :test_ids)

  def test_replays_history_then_resumes_shared_queue
    shared_queue = SharedQueue.new(['TestC#test_1'])
    history = History.new(3, ['TestA#test_1', 'TestB#test_1'])
    queue = CI::Queue::WorkerHistoryRecovery.new(shared_queue, history)
    queue.populate(tests('TestA#test_1', 'TestB#test_1', 'TestC#test_1'))

    order = []
    queue.poll do |test|
      order << test.id
      queue.acknowledge(test)
    end

    assert_equal ['TestA#test_1', 'TestB#test_1', 'TestC#test_1'], order
    assert_equal ['TestC#test_1'], shared_queue.acknowledged
    assert queue.replay_completed?
    assert queue.resumed_shared_queue?
    assert_equal 3, queue.history_items
    assert_equal 2, queue.replayed_tests
  end

  def test_replay_requeue_does_not_mutate_shared_queue
    shared_queue = SharedQueue.new(['TestB#test_1'])
    history = History.new(1, ['TestA#test_1'])
    queue = CI::Queue::WorkerHistoryRecovery.new(shared_queue, history)
    queue.populate(tests('TestA#test_1', 'TestB#test_1'))

    queue.poll do |test|
      queue.requeue(test) if test.id == 'TestA#test_1'
      queue.acknowledge(test)
    end

    assert_equal [], shared_queue.requeued
    assert_equal ['TestB#test_1'], shared_queue.acknowledged
  end

  def test_failed_replay_does_not_resume_shared_queue
    config = CI::Queue::Configuration.new(max_consecutive_failures: 1)
    shared_queue = SharedQueue.new(['TestC#test_1'], config: config)
    history = History.new(2, ['TestA#test_1', 'TestB#test_1'])
    queue = CI::Queue::WorkerHistoryRecovery.new(shared_queue, history)
    queue.populate(tests('TestA#test_1', 'TestB#test_1', 'TestC#test_1'))

    order = []
    queue.poll do |test|
      order << test.id
      queue.report_failure!
      queue.acknowledge(test)
    end

    assert_equal ['TestA#test_1'], order
    refute queue.replay_completed?
    refute queue.resumed_shared_queue?
    assert_equal [], shared_queue.acknowledged
  end

  def test_missing_replay_test_fails_before_resuming_shared_queue
    shared_queue = SharedQueue.new(['TestB#test_1'])
    history = History.new(1, ['MissingTest#test_1'])
    queue = CI::Queue::WorkerHistoryRecovery.new(shared_queue, history)
    queue.populate(tests('TestB#test_1'))

    assert_raises(KeyError) do
      queue.poll { |test| queue.acknowledge(test) }
    end

    refute queue.replay_completed?
    refute queue.resumed_shared_queue?
    assert_equal [], shared_queue.acknowledged
  end

  private

  def tests(*ids)
    ids.map { |id| Test.new(id) }
  end

  class SharedQueue
    attr_reader :acknowledged, :config, :requeued, :total

    def initialize(ids, config: CI::Queue::Configuration.new)
      @ids = ids
      @config = config
      @acknowledged = []
      @requeued = []
      @total = ids.size
    end

    def populate(tests, **_options)
      @index = tests.map { |test| [test.id, test] }.to_h
      self
    end

    def populated?
      defined?(@index)
    end

    def poll
      @ids.each { |id| yield @index.fetch(id) }
      @ids.clear
    end

    def acknowledge(test)
      @acknowledged << test.id
      true
    end

    def requeue(test, **)
      @requeued << test.id
      true
    end

    def increment_test_failed
      @test_failed = test_failed + 1
    end

    def test_failed
      @test_failed ||= 0
    end

    def exhausted?
      @ids.empty?
    end

    def size
      @ids.size
    end

    def progress
      total - size
    end

    def to_a
      @ids.map { |id| @index.fetch(id) }
    end

    def build
      @build ||= CI::Queue::BuildRecord.new(self)
    end

    def report_failure!
      config.circuit_breakers.each(&:report_failure!)
    end

    def report_success!
      config.circuit_breakers.each(&:report_success!)
    end

    def flaky?(_test)
      false
    end
  end
end
