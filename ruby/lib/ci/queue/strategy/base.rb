# frozen_string_literal: true

module CI
  module Queue
    module Strategy
      class Base
        def initialize(config)
          @config = config
        end

        attr_accessor :config

        def order_tests(tests, random: Random.new, config: nil, redis: nil)
          raise NotImplementedError, "#{self.class} must implement #order_tests"
        end
      end
    end
  end
end
