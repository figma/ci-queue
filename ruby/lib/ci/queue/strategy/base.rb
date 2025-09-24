# frozen_string_literal: true

module CI
  module Queue
    module Strategy
      class Base
        def order_tests(tests, random: Random.new, config: nil)
          raise NotImplementedError, "#{self.class} must implement #order_tests"
        end
      end
    end
  end
end