# frozen_string_literal: true
require_relative 'base'

module CI
  module Queue
    module Strategy
      class Random < Base
        def order_tests(tests, random: Random.new, config: nil, redis: nil)
          tests.sort.shuffle(random: random)
        end
      end
    end
  end
end