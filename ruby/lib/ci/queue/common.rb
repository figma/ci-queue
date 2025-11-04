# frozen_string_literal: true
module CI
  module Queue
    module Common
      attr_reader :config

      # to override in classes including this module
      CONNECTION_ERRORS = [].freeze

      def distributed?
        raise NotImplementedError
      end

      def retrying?
        false
      end

      def release!
        # noop
      end

      def flaky?(test)
        @config.flaky?(test)
      end

      def report_failure!
        config.circuit_breakers.each(&:report_failure!)
      end

      def report_success!
        config.circuit_breakers.each(&:report_success!)
      end

      def rescue_connection_errors(handler = ->(err) { nil })
        yield
      rescue *self::class::CONNECTION_ERRORS => err
        handler.call(err)
      end

      def ordering_strategy
        case config.strategy.to_sym
        when :timing_based
          Strategy::TimingBased.new(config)
        when :suite_bin_packing
          # pass redis if available
          # need to think about a better way to structure queue/strategy interaction
          redis_instance = if self.respond_to?(:redis, true) # include private methods
            self.send(:redis)
          else
            nil
          end
          Strategy::SuiteBinPacking.new(config, redis: redis_instance)
        else
          Strategy::Random.new(config)
        end
      end

      def reorder_tests(tests, random: Random.new)
        ordering_strategy.order_tests(tests, random: random)
      end
    end
  end
end
