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
            transaction.llen(generation_key('queue'))
            transaction.zcard(generation_key('running'))
          end.inject(:+)
        end

        def to_a
          redis.multi do |transaction|
            transaction.lrange(generation_key('queue'), 0, -1)
            transaction.zrange(generation_key('running'), 0, -1)
          end.flatten.reverse.map { |k| index.fetch(k) }
        end

        def progress
          total - size
        end

        def wait_for_master(timeout: 120)
          return true if master?

          deadline = CI::Queue.time_now + timeout
          last_status = nil

          while CI::Queue.time_now < deadline
            status = master_status

            # Success - queue is ready
            if status == 'ready' || status == 'finished'
              learn_generation unless master?
              return true
            end

            # Master lock expired during setup (died mid-population)
            # Status will be nil if lock expired
            if status.nil? && last_status == 'setup'
              raise MasterDied, "Master lock expired during setup - master may have died"
            end

            last_status = status
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

        private

        attr_reader :redis, :redis_url

        def key(*args)
          ['build', build_id, *args].join(':')
        end

        def build_id
          config.build_id
        end

        def master_status
          status = redis.get(key('master-status'))
          # Handle new format "setup:#{generation}" - return just "setup" for compatibility
          return 'setup' if status&.start_with?('setup:')
          status
        end

        def generation_key(*args)
          gen = @generation || @current_generation
          return key(*args) unless gen  # Fallback for backwards compatibility
          key('gen', gen, *args)
        end

        def learn_generation
          @current_generation = redis.get(key('current-generation'))
          raise MasterDied, "No generation available - master may have died" unless @current_generation
          @current_generation
        end

        def current_generation
          @generation || @current_generation
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
