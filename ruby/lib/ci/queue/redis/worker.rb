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
          executables = Queue.shuffle(tests, random, config: config)

          # Separate chunks from individual tests
          chunks = executables.select { |e| e.is_a?(CI::Queue::TestChunk) }
          individual_tests = executables.select { |e| !e.is_a?(CI::Queue::TestChunk) }

          # Store chunk metadata in Redis (only master does this)
          store_chunk_metadata(chunks) if chunks.any?

          # Push all IDs to queue (chunks + individual tests)
          all_ids = chunks.map(&:id) + individual_tests.map(&:id)
          push(all_ids)

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
            if id = reserve
              idle_since = nil
              executable = resolve_executable(id)

              if executable
                yield executable
              else
                warn("Warning: Could not resolve executable for ID #{id.inspect}. Acknowledging to remove from queue.")
                acknowledge(id)
              end
            else
              idle_since ||= CI::Queue.time_now
              if CI::Queue.time_now - idle_since > 120 && !idle_state_printed
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

        def acknowledge(test_or_id)
          # Accept either an object with .id or a string ID
          test_key = test_or_id.respond_to?(:id) ? test_or_id.id : test_or_id
          raise_on_mismatching_test(test_key)
          eval_script(
            :acknowledge,
            keys: [key('running'), key('processed'), key('owners')],
            argv: [test_key],
          ) == 1
        end

        def requeue(test, offset: Redis.requeue_offset, skip_reservation_check: false)
          test_key = test.id
          raise_on_mismatching_test(test_key) unless skip_reservation_check
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

          @reserved_test = test_key unless requeued || skip_reservation_check
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
              key('test-group-timeout'),
            ],
            argv: [CI::Queue.time_now.to_f, 'true', config.timeout],
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
              key('test-group-timeout'),
            ],
            argv: [CI::Queue.time_now.to_f, timeout, 'true', config.timeout],
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

        private

        def store_chunk_metadata(chunks)
          # Batch operations to avoid exceeding Redis multi operation limits
          # Each chunk requires 4 commands (set, expire, sadd, hset), so batch conservatively
          batch_size = 5 # 5 chunks = 20 commands + 2 expires = 22 commands per batch

          chunks.each_slice(batch_size) do |chunk_batch|
            redis.multi do |transaction|
              chunk_batch.each do |chunk|
                # Store chunk metadata with TTL
                transaction.set(
                  key('chunk', chunk.id),
                  chunk.to_json
                )
                transaction.expire(key('chunk', chunk.id), config.redis_ttl)

                # Track all chunks for cleanup
                transaction.sadd(key('chunks'), chunk.id)

                # Store dynamic timeout for this chunk
                # Timeout = default_timeout * number_of_tests
                chunk_timeout = config.timeout * chunk.test_count
                transaction.hset(key('test-group-timeout'), chunk.id, chunk_timeout)
              end
              transaction.expire(key('chunks'), config.redis_ttl)
              transaction.expire(key('test-group-timeout'), config.redis_ttl)
            end
          end
        end

        def chunk_id?(id)
          id.include?(':full_suite') || id.include?(':chunk_')
        end

        def resolve_executable(id)
          # Detect chunk by ID pattern
          if chunk_id?(id)
            resolve_chunk(id)
          else
            # Regular test - existing behavior
            index.fetch(id)
          end
        end

        def resolve_chunk(chunk_id)
          # Fetch chunk metadata from Redis
          chunk_json = redis.get(key('chunk', chunk_id))
          unless chunk_json
            warn "Warning: Chunk metadata not found for #{chunk_id}"
            return nil
          end

          chunk = CI::Queue::TestChunk.from_json(chunk_id, chunk_json)

          # Resolve test objects based on chunk type
          test_objects = if chunk.full_suite?
            resolve_full_suite_tests(chunk.suite_name)
          else
            resolve_partial_suite_tests(chunk.test_ids)
          end

          if test_objects.empty?
            warn "Warning: No tests found for chunk #{chunk_id}"
            return nil
          end

          # Return enriched chunk with actual test objects
          ResolvedChunk.new(chunk, test_objects)
        rescue JSON::ParserError => e
          warn "Warning: Could not parse chunk metadata for #{chunk_id}: #{e.message}"
          nil
        rescue KeyError => e
          warn "Warning: Could not resolve test in chunk #{chunk_id}: #{e.message}"
          nil
        end

        def resolve_full_suite_tests(suite_name)
          # Filter index for all tests from this suite
          # Tests are added to index during populate() with format "SuiteName#test_method"
          prefix = "#{suite_name}#"
          tests = index.select { |test_id, _| test_id.start_with?(prefix) }
                        .values

          # Sort to maintain consistent order (alphabetical by test name)
          tests.sort_by(&:id)
        end

        def resolve_partial_suite_tests(test_ids)
          # Fetch specific tests from index
          test_ids.map { |test_id| index.fetch(test_id) }
        end

        # Represents a chunk with resolved test objects
        class ResolvedChunk
          attr_reader :chunk_id, :suite_name, :tests

          def initialize(chunk, tests)
            @chunk_id = chunk.id
            @suite_name = chunk.suite_name
            @tests = tests.freeze
          end

          def id
            chunk_id
          end

          def chunk?
            true
          end

          def flaky?
            tests.any?(&:flaky?)
          end

          def size
            tests.size
          end
        end
      end
    end
  end
end
