# frozen_string_literal: true

require 'ci/queue/static'
require 'concurrent/set'
require 'digest/sha2'
require 'set'

module CI
  module Queue
    module Redis
      ReservationError = Class.new(StandardError)

      class << self
        attr_accessor :requeue_offset
        attr_accessor :max_sleep_time
      end
      self.requeue_offset = 42
      self.max_sleep_time = 2

      class Worker < Base
        DEFAULT_SLEEP_SECONDS = 0.5
        attr_reader :total

        def initialize(redis, config)
          @reserved_test = nil
          @reserved_tests = Concurrent::Set.new
          @shutdown_required = false
          @idle_since = nil
          super(redis, config)
        end

        attr_accessor :idle_since

        def distributed?
          true
        end

        def populate(tests, random: Random.new)
          # All workers need an index of tests to resolve IDs
          @index = tests.map { |t| [t.id, t] }.to_h
          @total = tests.size
          @random = random
          @tests = tests

          if acquire_master_role?
            warn "Master acquired role. Sleeping for 45 seconds to simulate a long setup."
            sleep 45
            warn "Master finished sleeping. should have lost role."
            execute_master_setup
          end

          register_worker_presence

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
          !@idle_since.nil?
        end

        def poll
          wait_for_master
          if master?
            warn "Worker #{worker_id} is the master"
          else
            master_id = master_worker_id
            warn "Worker #{worker_id} saw master worker: #{master_id}" if master_id
          end
          idle_since = nil
          idle_state_printed = false
          attempt = 0
          until shutdown_required? || config.circuit_breakers.any?(&:open?) || exhausted? || max_test_failed?
            if id = reserve
              attempt = 0
              idle_since = nil
              executable = resolve_executable(id)

              if executable
                with_heartbeat(id) do
                  yield executable
                end
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
                  puts '  Checking if running tests are in processed set:'
                  running_tests.each do |test, _score|
                    puts "    #{test}: #{redis.sismember(key('processed'), test)}"
                  end
                end
                idle_state_printed = true
              end
              # Adding exponential backoff to avoid hammering Redis
              # we just stay online here in case a test gets retried or times out so we can afford to wait
              sleep_time = [DEFAULT_SLEEP_SECONDS * (2 ** attempt), Redis.max_sleep_time].min
              attempt += 1
              sleep sleep_time
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

          max_retries = 5
          retry_count = 0

          begin
            eval_script(
              :acknowledge,
              keys: [key('running'), key('processed'), key('owners')],
              argv: [test_key]
            ) == 1
          rescue StandardError => e
            retry_count += 1
            if retry_count < max_retries
              # Exponential backoff: 1s, 2s, ...
              sleep(0.1 * (2**(retry_count - 1)))
              retry
            else
              warn("Failed to acknowledge test #{test_key.inspect} after #{max_retries} retries: #{e.class} - #{e.message}. " \
                   'Test remains in running set and may be picked up by reserve_lost after timeout.')
              raise
            end
          end
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
              key('owners')
            ],
            argv: [config.max_requeues, global_max_requeues, test_key, offset]
          ) == 1

          @reserved_test = test_key unless requeued || skip_reservation_check
          requeued
        end

        def release!
          eval_script(
            :release,
            keys: [key('running'), key('worker', worker_id, 'queue'), key('owners')],
            argv: []
          )
          nil
        end

        # Send a heartbeat for the currently reserved test to indicate the worker is still active.
        # This extends the deadline and prevents other workers from stealing the test.
        # Returns true if heartbeat was successful, false if test was already processed or
        # we're no longer the owner.
        def heartbeat(test_or_id = nil)
          test_key = if test_or_id
                       test_or_id.respond_to?(:id) ? test_or_id.id : test_or_id
                     else
                       @reserved_test
                     end
          return false unless test_key

          current_time = CI::Queue.time_now.to_f
          result = eval_script(
            :heartbeat,
            keys: [
              key('running'),
              key('processed'),
              key('owners'),
              key('worker', worker_id, 'queue'),
              key('heartbeats'),
              key('test-group-timeout')
            ],
            argv: [current_time, test_key, config.timeout]
          )

          if result.is_a?(Array) && result.length == 2
            new_deadline, timeout_used = result
            current_time_readable = Time.at(current_time).strftime('%Y-%m-%d %H:%M:%S')
            deadline_readable = Time.at(new_deadline).strftime('%Y-%m-%d %H:%M:%S')
            warn "[heartbeat] test=#{test_key} current_time=#{current_time_readable} extended_deadline=#{deadline_readable} timeout=#{timeout_used}s"
            true
          else
            false
          end
        rescue *CONNECTION_ERRORS
          false
        end

        private

        attr_reader :index

        # Runs a block while sending periodic heartbeats in a background thread.
        # This prevents other workers from stealing the test while it's being executed.
        def with_heartbeat(test_id)
          return yield unless config.heartbeat_interval&.positive?

          # Pre-initialize Redis connection and script in current thread context
          # This ensures background threads use the same initialized connection
          ensure_connection_and_script(:heartbeat)

          stop_heartbeat = false
          heartbeat_thread = Thread.new do
            until stop_heartbeat
              sleep(config.heartbeat_interval)
              break if stop_heartbeat

              begin
                heartbeat(test_id)
              rescue StandardError => e
                warn("[heartbeat] Failed to send heartbeat for #{test_id}: #{e.message}")
              end
            end
          end

          yield
        ensure
          stop_heartbeat = true
          heartbeat_thread&.kill
          heartbeat_thread&.join(1) # Wait up to 1 second for thread to finish
        end

        # Runs a block while sending periodic heartbeats during master setup.
        # This allows other workers to detect if the master dies during setup.
        def with_master_setup_heartbeat
          return yield unless config.master_setup_heartbeat_interval&.positive?

          # Send initial heartbeat immediately
          send_master_setup_heartbeat

          stop_heartbeat = false
          heartbeat_thread = Thread.new do
            until stop_heartbeat
              sleep(config.master_setup_heartbeat_interval)
              break if stop_heartbeat

              begin
                send_master_setup_heartbeat
              rescue StandardError => e
                warn("[master-setup-heartbeat] Failed to send heartbeat: #{e.message}")
              end
            end
          end

          yield
        ensure
          stop_heartbeat = true
          heartbeat_thread&.kill
          heartbeat_thread&.join(1)
        end

        # Send a heartbeat to indicate master is still alive during setup
        def send_master_setup_heartbeat
          current_time = CI::Queue.time_now.to_f
          redis.set(key('master-setup-heartbeat'), current_time)
          redis.expire(key('master-setup-heartbeat'), config.redis_ttl)
        rescue *CONNECTION_ERRORS => e
          warn("[master-setup-heartbeat] Connection error: #{e.message}")
        end

        def ensure_connection_and_script(script)
          # Pre-initialize Redis connection and script in current thread context
          # This ensures background threads use the same initialized connection
          load_script(script)
          # Ping Redis to ensure connection is established
          redis.ping
        end

        def reserved_tests
          @reserved_tests ||= Concurrent::Set.new
        end

        def worker_id
          config.worker_id
        end

        def timeout
          config.timeout
        end

        def raise_on_mismatching_test(test)
          unless @reserved_test == test
            raise ReservationError, "Acknowledged #{test.inspect} but #{@reserved_test.inspect} was reserved"
          end

          @reserved_test = nil
        end

        def reserve
          if @reserved_test
            raise ReservationError, "#{@reserved_test.inspect} is already reserved. " \
              'You have to acknowledge it before you can reserve another one'
          end

          test = try_to_reserve_lost_test || try_to_reserve_test
          @reserved_test = test if test
          test
        end

        def try_to_reserve_test
          current_time = CI::Queue.time_now.to_f
          test_id = eval_script(
            :reserve,
            keys: [
              key('queue'),
              key('running'),
              key('processed'),
              key('worker', worker_id, 'queue'),
              key('owners'),
              key('test-group-timeout')
            ],
            argv: [current_time, 'true', config.timeout]
          )

          if test_id
            # Check what timeout was used (dynamic or default)
            dynamic_timeout = redis.hget(key('test-group-timeout'), test_id)
            timeout_used = dynamic_timeout ? dynamic_timeout.to_f : config.timeout
            deadline = current_time + timeout_used
            gap_seconds = timeout_used
            gap_hours = (gap_seconds / 3600).to_i
            gap_mins = ((gap_seconds % 3600) / 60).to_i
            gap_secs = (gap_seconds % 60)

            current_time_readable = Time.at(current_time).strftime('%Y-%m-%d %H:%M:%S')
            deadline_readable = Time.at(deadline).strftime('%Y-%m-%d %H:%M:%S')

            # Format gap_seconds to 2 decimal places, and gap_secs for the formatted version
            gap_seconds_formatted = format('%.2f', gap_seconds)
            gap_secs_formatted = gap_secs < 60 ? format('%.2f', gap_secs) : gap_secs.to_i.to_s

            # Add details about how timeout was computed
            timeout_details = if dynamic_timeout
                                # For chunks, calculate back to estimated_duration (without buffer)
                                estimated_duration_ms = (dynamic_timeout.to_f / 1.1 * 1000).round
                                estimated_duration_seconds = estimated_duration_ms / 1000.0
                                "dynamic_timeout=#{dynamic_timeout.to_f}s (estimated_duration=#{estimated_duration_seconds}s * 1.1 buffer)"
                              else
                                "default_timeout=#{config.timeout}s"
                              end

            warn "[reserve] test=#{test_id} current_time=#{current_time_readable} (#{current_time}) deadline=#{deadline_readable} (#{deadline}) gap=#{gap_seconds_formatted}s (#{gap_hours}h#{gap_mins}m#{gap_secs_formatted}s) [#{timeout_details}]"
          end

          test_id
        end

        def try_to_reserve_lost_test
          current_time = CI::Queue.time_now.to_f
          lost_test = eval_script(
            :reserve_lost,
            keys: [
              key('running'),
              key('completed'),
              key('worker', worker_id, 'queue'),
              key('owners'),
              key('test-group-timeout'),
              key('heartbeats')
            ],
            argv: [current_time, timeout, 'true', config.timeout, config.heartbeat_grace_period]
          )

          if lost_test
            # Check what timeout was used (dynamic or default)
            dynamic_timeout = redis.hget(key('test-group-timeout'), lost_test)
            timeout_used = dynamic_timeout ? dynamic_timeout.to_f : config.timeout
            deadline = current_time + timeout_used
            gap_seconds = timeout_used
            gap_hours = (gap_seconds / 3600).to_i
            gap_mins = ((gap_seconds % 3600) / 60).to_i
            gap_secs = (gap_seconds % 60)

            current_time_readable = Time.at(current_time).strftime('%Y-%m-%d %H:%M:%S')
            deadline_readable = Time.at(deadline).strftime('%Y-%m-%d %H:%M:%S')

            # Format gap_seconds to 2 decimal places, and gap_secs for the formatted version
            gap_seconds_formatted = format('%.2f', gap_seconds)
            gap_secs_formatted = gap_secs < 60 ? format('%.2f', gap_secs) : gap_secs.to_i.to_s

            # Add details about how timeout was computed
            timeout_details = if dynamic_timeout
                                # For chunks, calculate back to estimated_duration (without buffer)
                                estimated_duration_ms = (dynamic_timeout.to_f / 1.1 * 1000).round
                                estimated_duration_seconds = estimated_duration_ms / 1000.0
                                "dynamic_timeout=#{dynamic_timeout.to_f}s (estimated_duration=#{estimated_duration_seconds}s * 1.1 buffer)"
                              else
                                "default_timeout=#{config.timeout}s"
                              end

            warn "[reserve_lost] test=#{lost_test} current_time=#{current_time_readable} (#{current_time}) deadline=#{deadline_readable} (#{deadline}) gap=#{gap_seconds_formatted}s (#{gap_hours}h#{gap_mins}m#{gap_secs_formatted}s) [#{timeout_details}]"
          end

          if lost_test.nil? && idle?
            puts "Worker #{worker_id} could not reserve a lost test while idle"
            puts 'Printing running tests:'
            puts "#{redis.zrange(key('running'), 0, -1, withscores: true)}"
          end

          build.record_warning(Warnings::RESERVED_LOST_TEST, test: lost_test, timeout: timeout) if lost_test

          lost_test
        end

        def push(tests)
          @total = tests.size
          return unless @master

          # Use WATCH/MULTI for atomic check-and-push to prevent TOCTOU race.
          # If master-worker-id changes between WATCH and MULTI, transaction aborts.
          result = redis.watch(key('master-worker-id')) do |rd|
            current_master = rd.get(key('master-worker-id'))

            if current_master && current_master != worker_id
              # We're not the master anymore, unwatch and abort
              rd.unwatch
              :not_master
            else
              # We're still master, execute atomic transaction
              rd.multi do |transaction|
                transaction.lpush(key('queue'), tests) unless tests.empty?
                transaction.set(key('total'), @total)
                transaction.set(key('master-status'), 'ready')

                transaction.expire(key('queue'), config.redis_ttl)
                transaction.expire(key('total'), config.redis_ttl)
                transaction.expire(key('master-status'), config.redis_ttl)
              end
            end
          end

          # result is nil if WATCH detected a change (race condition)
          # result is :not_master if we detected we're not master
          # result is an array of responses if transaction succeeded
          if result.nil? || result == :not_master
            warn "Worker #{worker_id} lost master role (race detected), aborting push"
            @master = false
          end
        rescue *CONNECTION_ERRORS
          raise if @master
        end

        def register
          redis.sadd(key('workers'), [worker_id])
        end

        def acquire_master_role?
          return true if @master

          @master = redis.setnx(key('master-status'), 'setup')
          if @master
            begin
              redis.set(key('master-worker-id'), worker_id)
              redis.expire(key('master-worker-id'), config.redis_ttl)

              # Set initial heartbeat immediately to prevent premature takeover
              # This closes the window where status='setup' but no heartbeat exists
              redis.set(key('master-setup-heartbeat'), CI::Queue.time_now.to_f)
              redis.expire(key('master-setup-heartbeat'), config.redis_ttl)

              warn "Worker #{worker_id} elected as master"
            rescue *CONNECTION_ERRORS
              # If setting master-worker-id/heartbeat fails, we still have master status
              # Log but don't lose master role
              warn("Failed to set master-worker-id: #{$!.message}")
            end
          end
          @master
        rescue *CONNECTION_ERRORS
          @master = nil
          false
        end

        # Attempt to take over as master when current master appears dead during setup.
        # Uses atomic Lua script to ensure only one worker can win the takeover.
        # Returns true if takeover succeeded, false otherwise.
        def attempt_master_takeover
          return false if @master # Already master

          warn "Worker #{worker_id} attempting to takeover as master from #{master_worker_id}"

          current_time = CI::Queue.time_now.to_f
          result = eval_script(
            :takeover_master,
            keys: [
              key('master-status'),
              key('master-worker-id'),
              key('master-setup-heartbeat')
            ],
            argv: [
              worker_id,
              current_time,
              config.master_setup_heartbeat_timeout,
              config.redis_ttl
            ]
          )

          if result == 1
            @master = true
            warn "Worker #{worker_id} took over as master (previous master died during setup)"
            true
          else
            warn "Failed to takeover as master. Current master is #{master_worker_id}"
            false
          end
        rescue *CONNECTION_ERRORS => e
          warn "[takeover] Connection error during takeover attempt: #{e.message}"
          false
        end

        def register_worker_presence
          register
          redis.expire(key('workers'), config.redis_ttl)
        rescue *CONNECTION_ERRORS
          raise if master?
        end

        # Shared logic for master setup - reorders tests, stores chunk metadata, and pushes to queue.
        # Used by both initial master setup (populate) and takeover.
        def execute_master_setup
          return unless @master && @index
          with_master_setup_heartbeat do
            executables = reorder_tests(@tests, random: @random)

            chunks = executables.select { |e| e.is_a?(CI::Queue::TestChunk) }
            individual_tests = executables.reject { |e| e.is_a?(CI::Queue::TestChunk) }

            store_chunk_metadata(chunks) if chunks.any?

            all_ids = chunks.map(&:id) + individual_tests.map(&:id)
            push(all_ids)
          end
        end

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
                # Timeout = estimated_duration (in ms) converted to seconds + buffer
                # estimated_duration is in milliseconds, convert to seconds and add 10% buffer
                buffer_percent = 10
                estimated_duration_seconds = chunk.estimated_duration / 1000.0
                chunk_timeout = (estimated_duration_seconds * (1 + buffer_percent / 100.0)).round(2)
                # Format to string to avoid floating point precision issues in Redis
                # Use %g to remove trailing zeros
                transaction.hset(key('test-group-timeout'), chunk.id, format('%g', chunk_timeout))
              end
              transaction.expire(key('chunks'), config.redis_ttl)
              transaction.expire(key('test-group-timeout'), config.redis_ttl)
            end
          end
        end

        def chunk_id?(id)
          id.include?(':chunk_')
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
          test_objects = resolve_suite_tests(chunk.test_ids)

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

        def resolve_suite_tests(test_ids)
          # Fetch specific tests from index
          test_ids.map { |test_id| index.fetch(test_id) }
        end

        # Represents a chunk with resolved test objects
        class ResolvedChunk
          attr_reader :chunk_id, :suite_name, :tests, :estimated_duration

          def initialize(chunk, tests)
            @chunk_id = chunk.id
            @suite_name = chunk.suite_name
            @tests = tests.freeze
            @estimated_duration = chunk.estimated_duration
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
