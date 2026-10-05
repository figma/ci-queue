# frozen_string_literal: true
require 'test_helper'

class CI::Queue::StaticTest < Minitest::Test
  include SharedQueueAssertions

  def test_shutdown_stops_polling
    tests = []

    @queue.poll do |test|
      tests << test
      @queue.shutdown!
    end

    assert_equal 1, tests.size
    refute_predicate @queue, :exhausted?
  end

  def test_expiry_uses_original_start_time
    @queue.created_at = 1000
    @queue.created_at = 2000

    CI::Queue.stub(:time_now, Time.at(1000)) do
      refute_predicate @queue, :expired?
    end
    CI::Queue.stub(:time_now, Time.at(1600)) do
      refute_predicate @queue, :expired?
    end
    CI::Queue.stub(:time_now, Time.at(1601)) do
      assert_predicate @queue, :expired?
    end
  end

  private

  def test_from_uri
    queue = CI::Queue.from_uri('list:foo:bar:plop%3Ffizz', config)
    assert_instance_of CI::Queue::Static, queue
    assert_equal %w(foo bar plop?fizz), queue.to_a
  end

  def build_queue
    CI::Queue::Static.new(TEST_LIST.map(&:id), config)
  end
end
