# frozen_string_literal: true

module CI
  module Queue
    module Redis
      class Base
        include Common

        TEN_MINUTES = 60 * 10
        CONNECTION_ERRORS = [
          ::Redis::BaseConnectionError,
          ::SocketError # https://github.com/redis/redis-rb/pull/631
        ].freeze

        def initialize(redis_url, config)
          @redis_url = redis_url
          @redis = ::Redis.new(url: redis_url)
          @config = config
        end

        def exhausted?
          queue_initialized? && size == 0
        end

        def expired?
          if (created_at = redis.get(key('created-at')))
            (created_at.to_f + config.redis_ttl + TEN_MINUTES) < CI::Queue.time_now.to_f
          else
            # if there is no created at set anymore we assume queue is expired
            true
          end
        end

        def created_at=(timestamp)
          redis.setnx(key('created-at'), timestamp)
        end

        def size
          redis.multi do |transaction|
            transaction.llen(key('queue'))
            transaction.zcard(key('running'))
          end.inject(:+)
        end

        def to_a
          redis.multi do |transaction|
            transaction.lrange(key('queue'), 0, -1)
            transaction.zrange(key('running'), 0, -1)
          end.flatten.reverse.map { |k| index.fetch(k) }
        end

        def progress
          total - size
        end

        def wait_for_master(timeout: 120)
          return true if master?

          last_takeover_check = CI::Queue.time_now.to_f

          (timeout * 10 + 1).to_i.times do
            return true if queue_initialized?

            # Periodically check if master is dead and attempt takeover
            current_time = CI::Queue.time_now.to_f
            if current_time - last_takeover_check >= config.master_setup_heartbeat_interval
              last_takeover_check = current_time

              if queue_initializing? && master_setup_heartbeat_stale?
                if respond_to?(:attempt_master_takeover, true) && attempt_master_takeover
                  # Takeover succeeded - run master setup
                  if respond_to?(:run_master_setup, true)
                    execute_master_setup
                    return true
                  end
                end
              end
            end

            sleep 0.1
          end
          raise LostMaster, "The master worker (worker #{master_worker_id}) is still `#{master_status}` after #{timeout} seconds waiting."
        end

        def workers_count
          redis.scard(key('workers'))
        end

        def queue_initialized?
          @queue_initialized ||= begin
            status = master_status
            %w[ready finished].include?(status)
          end
        end

        def queue_initializing?
          master_status == 'setup'
        end

        def increment_test_failed
          redis.incr(key('test_failed_count'))
        end

        def test_failed
          redis.get(key('test_failed_count')).to_i
        end

        def max_test_failed?
          return false if config.max_test_failed.nil?

          test_failed >= config.max_test_failed
        end

        def master_worker_id
          redis.get(key('master-worker-id'))
        end

        # Check if the master setup heartbeat is stale (or missing)
        # Returns true if heartbeat is older than master_setup_heartbeat_timeout
        def master_setup_heartbeat_stale?
          heartbeat = redis.get(key('master-setup-heartbeat'))
          return true unless heartbeat # No heartbeat = stale (master may have died before first heartbeat)

          current_time = CI::Queue.time_now.to_f
          heartbeat_age = current_time - heartbeat.to_f
          heartbeat_age >= config.master_setup_heartbeat_timeout
        rescue *CONNECTION_ERRORS
          false # On connection error, don't attempt takeover
        end

        private

        attr_reader :redis, :redis_url

        def key(*args)
          ['build', build_id, *args].join(':')
        end

        def build_id
          config.build_id
        end

        def master_status
          redis.get(key('master-status'))
        end

        def eval_script(script, *args)
          redis.evalsha(load_script(script), *args)
        end

        def load_script(script)
          @scripts_cache ||= {}
          @scripts_cache[script] ||= redis.script(:load, read_script(script))
        end

        def read_script(name)
          ::File.read(::File.join(CI::Queue::DEV_SCRIPTS_ROOT, "#{name}.lua"))
        rescue SystemCallError
          ::File.read(::File.join(CI::Queue::RELEASE_SCRIPTS_ROOT, "#{name}.lua"))
        end
      end
    end
  end
end
