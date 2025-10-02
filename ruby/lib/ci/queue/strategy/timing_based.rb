# frozen_string_literal: true
require_relative 'base'
require 'json'

module CI
  module Queue
    module Strategy
      class TimingBased < Base
        def order_tests(tests, random: Random.new, config: nil)
          timing_data = load_timing_data(config&.timing_file)
          fallback_duration = config&.timing_fallback_duration || 100.0

          tests.sort_by do |test|
            duration = timing_data[test.id] || fallback_duration
            -duration # Negative for descending order (longest first)
          end
        end

        private

        def load_timing_data(file_path)
          return {} unless file_path && ::File.exist?(file_path)
          
          JSON.parse(::File.read(file_path))
        rescue JSON::ParserError => e
          warn "Warning: Could not parse timing file #{file_path}: #{e.message}"
          {}
        rescue => e
          warn "Warning: Could not read timing file #{file_path}: #{e.message}"
          {}
        end
      end
    end
  end
end