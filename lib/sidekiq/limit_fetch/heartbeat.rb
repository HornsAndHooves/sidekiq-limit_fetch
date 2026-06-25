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
          thread.join(LimitFetch.configuration[:heartbeat_period] * 2)
        end
      end

      # @param capsule [Sidekiq::Capsule]
      def initialize(capsule)
        @capsule = capsule
        @down = false
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
            with_error_handling do
              capsule_sem.heartbeat
              if @down
                log :info, "Redis back online, heartbeat completed successfully"
                @down = false
              end
            end
            with_error_handling { Kernel.sleep(LimitFetch.configuration[:heartbeat_period]) }

          rescue LimitFetch::Shutdown
            break
          end
        end

        with_error_handling do
          capsule_sem.purge([capsule_meta.uuid])  # Deregister
          log :info, "Successfully shut down"
        end
      end

      # @return [Boolean] whether block completed successfully
      def with_error_handling
        yield
        true
      rescue LimitFetch::Shutdown
        raise
      rescue StandardError => error
        handle_error(error)
        false
      end

      # @param error [StandardError]
      def handle_error(error)
        if @down
          # Already logged the original error, make repeating connection errors more concise
          log :error, "Failed: #{error.class}"
        else
          log :error, "#{error.class}: #{error.message}#{error.backtrace.join("\n")}"
        end
        @down = true
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
