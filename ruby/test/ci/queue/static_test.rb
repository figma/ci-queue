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
