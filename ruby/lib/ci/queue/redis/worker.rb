# frozen_string_literal: true
require 'ci/queue/static'
require 'set'

module CI
  module Queue
    module Redis
      ReservationError = Class.new(StandardError)

      class << self
        attr_accessor :requeue_offset
      end
      self.requeue_offset = 42

      class Worker < Base
        attr_reader :total

        def initialize(redis, config)
          @reserved_test = nil
          @shutdown_required = false
          @idle_since = nil
          super(redis, config)
        end

        attr_accessor :idle_since

        def distributed?
          true
        end

        def populate(tests, random: Random.new)
          @index = tests.map { |t| [t.id, t] }.to_h
          tests = Queue.shuffle(tests, random)
          push(tests.map(&:id))
          self
        end

        def populated?
          !!defined?(@index)
        end

        def shutdown!
          @shutdown_required = true
        end

        def shutdown_required?
          @shutdown_required
        end

        def master?
          @master
        end

        def idle?
          !(@idle_since.nil?)
        end

        def poll
          wait_for_master
          idle_since = nil
          idle_state_printed = false
          until shutdown_required? || config.circuit_breakers.any?(&:open?) || exhausted? || max_test_failed?
            if test = reserve
              idle_since = nil
              yield index.fetch(test)
            else
              idle_since ||= Time.now
              if Time.now - idle_since > 120 && !idle_state_printed
                puts "Worker #{worker_id} has been idle for 120 seconds. Printing global state..."
                running_tests = redis.zrange(key('running'), 0, -1, withscores: true)
                puts "  Processed tests: #{redis.scard(key('processed'))}"
                puts "  Pending tests: #{redis.llen(key('queue'))}. #{redis.lrange(key('queue'), 0, -1)}"
                puts "  Running tests: #{running_tests.size}. #{running_tests}"
                puts "  Owners: #{redis.hgetall(key('owners'))}"
                unless running_tests.empty?
                  puts "  Checking if running tests are in processed set:"
                  running_tests.each do |test, _score|
                    puts "    #{test}: #{redis.sismember(key('processed'), test)}"
                  end
                end
                idle_state_printed = true
              end
              sleep 0.05
            end
          end
          redis.pipelined do |pipeline|
            pipeline.expire(key('worker', worker_id, 'queue'), config.redis_ttl)
            pipeline.expire(key('processed'), config.redis_ttl)
          end
        rescue *CONNECTION_ERRORS
        end

        if ::Redis.method_defined?(:exists?)
          def retrying?
            redis.exists?(key('worker', worker_id, 'queue'))
          rescue *CONNECTION_ERRORS
            false
          end
        else
          def retrying?
            redis.exists(key('worker', worker_id, 'queue'))
          rescue *CONNECTION_ERRORS
            false
          end
        end

        def retry_queue
          failures = build.failed_tests.to_set
          log = redis.lrange(key('worker', worker_id, 'queue'), 0, -1)
          log.select! { |id| failures.include?(id) }
          log.uniq!
          log.reverse!
          Retry.new(log, config, redis: redis)
        end

        def supervisor
          Supervisor.new(redis_url, config)
        end

        def build
          @build ||= CI::Queue::Redis::BuildRecord.new(self, redis, config)
        end

        def acknowledge(test)
          test_key = test.id
          raise_on_mismatching_test(test_key)
          eval_script(
            :acknowledge,
            keys: [key('running'), key('processed'), key('owners')],
            argv: [test_key],
          ) == 1
        end

        def requeue(test, offset: Redis.requeue_offset)
          test_key = test.id
          raise_on_mismatching_test(test_key)
          global_max_requeues = config.global_max_requeues(total)

          requeued = config.max_requeues > 0 && global_max_requeues > 0 && !config.known_flaky?(test_key) && eval_script(
            :requeue,
            keys: [
              key('processed'),
              key('requeues-count'),
              key('queue'),
              key('running'),
              key('worker', worker_id, 'queue'),
              key('owners'),
            ],
            argv: [config.max_requeues, global_max_requeues, test_key, offset],
          ) == 1

          @reserved_test = test_key unless requeued
          requeued
        end

        def release!
          eval_script(
            :release,
            keys: [key('running'), key('worker', worker_id, 'queue'), key('owners')],
            argv: [],
          )
          nil
        end

        private

        attr_reader :index

        def worker_id
          config.worker_id
        end

        def timeout
          config.timeout
        end

        def raise_on_mismatching_test(test)
          if @reserved_test == test
            @reserved_test = nil
          else
            raise ReservationError, "Acknowledged #{test.inspect} but #{@reserved_test.inspect} was reserved"
          end
        end

        def reserve
          if @reserved_test
            raise ReservationError, "#{@reserved_test.inspect} is already reserved. " \
              "You have to acknowledge it before you can reserve another one"
          end

          @reserved_test = (try_to_reserve_lost_test || try_to_reserve_test)
        end

        def try_to_reserve_test
          eval_script(
            :reserve,
            keys: [
              key('queue'),
              key('running'),
              key('processed'),
              key('worker', worker_id, 'queue'),
              key('owners'),
            ],
            argv: [Time.now.to_f],
          )
        end

        def try_to_reserve_lost_test
          lost_test = eval_script(
            :reserve_lost,
            keys: [
              key('running'),
              key('completed'),
              key('worker', worker_id, 'queue'),
              key('owners'),
            ],
            argv: [Time.now.to_f, timeout],
          )

          if lost_test.nil? && idle?
            puts "Worker #{worker_id} could not reserve a lost test while idle"
            puts "Printing running tests:"
            puts "#{redis.zrange(key('running'), 0, -1, withscores: true)}"
          end

          if lost_test
            build.record_warning(Warnings::RESERVED_LOST_TEST, test: lost_test, timeout: timeout)
          end

          lost_test
        end

        def push(tests)
          @total = tests.size

          if @master = redis.setnx(key('master-status'), 'setup')
            redis.multi do |transaction|
              transaction.lpush(key('queue'), tests) unless tests.empty?
              transaction.set(key('total'), @total)
              transaction.set(key('master-status'), 'ready')

              transaction.expire(key('queue'), config.redis_ttl)
              transaction.expire(key('total'), config.redis_ttl)
              transaction.expire(key('master-status'), config.redis_ttl)
            end
          end
          register
          redis.expire(key('workers'), config.redis_ttl)
        rescue *CONNECTION_ERRORS
          raise if @master
        end

        def register
          redis.sadd(key('workers'), [worker_id])
        end
      end
    end
  end
end
