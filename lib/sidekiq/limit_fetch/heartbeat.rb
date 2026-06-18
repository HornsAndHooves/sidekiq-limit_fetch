# frozen_string_literal: true

module Sidekiq
  class LimitFetch
    # Manages lifecycle of the heartbeat thread.
    class Heartbeat
      # @return [Set<Thread>]
      def self.threads
        @threads ||= Set.new
      end

      # Send a shutdown notification to all heartbeat threads.
      def self.shutdown
        threads.each do |thread|
          thread.raise(Shutdown)
          thread.join
        end
      end

      # @param capsule [Sidekiq::Capsule]
      def initialize(capsule)
        @capsule = capsule
        @redis_down = false
      end

      # @return [Thread]
      def start
        Thread.new { run }.tap do |thread|
          thread.name = "sidekiq-limit_fetch.heartbeat"
          self.class.threads << thread
        end
      end

      # Run a heartbeat every 15s.
      # Some special error handling is included to be able to endure a Redis outage.
      def run
        loop do
          begin
            capsule_sem.heartbeat
            if @redis_down
              log :info, "Redis back online, heartbeat completed successfully"
              @redis_down = false
            end
          rescue RedisClient::Error => error
            handle_redis_error(error)
          end

          Kernel.sleep(HEARTBEAT_PERIOD)
        rescue LimitFetch::Shutdown
          break
        end

        log :info, "Shutting down"
        if !@redis_down
          capsule_sem.purge([capsule_meta.uuid])  # Deregister
        end
      end

      # @param error [RedisClient::Error]
      def handle_redis_error(error)
        if @redis_down
          # Already logged the original error, make repeating connection errors more concise
          log :error, "Failed: #{error.class}"
        else
          log :error, "#{error.class}: #{error.message}#{error.backtrace.join("\n")}"
        end
        @redis_down = true
      end

      # @return [Global::CapsuleSemaphor]
      def capsule_sem
        @capsule_sem ||= Global::CapsuleSemaphor.new(@capsule)
      end

      # @return [Global::CapsuleMetadata]
      def capsule_meta
        @capsule_meta ||= Global.capsule[@capsule.name]
      end

      # @param level   [#to_s]
      # @param message [#to_s]
      def log(level, message)
        Sidekiq.logger.public_send(level, "[sidekiq-limit_fetch] [heartbeat] #{message}")
      end
    end
  end
end
