# frozen_string_literal: true
module CI
  module Queue
    module Redis
      class Supervisor < Base
        def master?
          false
        end

        def total
          wait_for_master(timeout: config.queue_init_timeout)
          redis.get(key('total')).to_i
        end

        def build
          @build ||= CI::Queue::Redis::BuildRecord.new(self, redis, config)
        end

        def wait_for_workers
          wait_for_master(timeout: config.queue_init_timeout)

          yield if block_given?

          time_left = config.report_timeout
          time_left_with_no_workers = config.inactive_workers_timeout
          last_heartbeat_time = Time.now
          until exhausted? || time_left <= 0 || max_test_failed? || time_left_with_no_workers <= 0
            time_left -= 1
            sleep 1

            # Heartbeat log every 5 minutes
            if Time.now - last_heartbeat_time > 300
              puts '[ci-queue] Still working'
              last_heartbeat_time = Time.now
            end

            if active_workers?
              time_left_with_no_workers = config.inactive_workers_timeout
            else
              time_left_with_no_workers -= 1
            end

            yield if block_given?
          end

          puts "Aborting, it seems all workers died." if time_left_with_no_workers <= 0
          exhausted?
        rescue CI::Queue::Redis::LostMaster
          false
        end

        private

        def active_workers?
          # if there are running jobs we assume there are still agents active
          redis.zrangebyscore(key('running'), Time.now.to_f - config.timeout, "+inf", limit: [0,1]).count > 0
        end
      end
    end
  end
end
