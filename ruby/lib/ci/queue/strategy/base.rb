# frozen_string_literal: true

module CI
  module Queue
    module Strategy
      class Base
        def initialize(config)
          @config = config
        end

        attr_reader :config

        def order_tests(tests)
          raise NotImplementedError, "#{self.class} must implement #order_tests"
        end
      end
    end
  end
end
