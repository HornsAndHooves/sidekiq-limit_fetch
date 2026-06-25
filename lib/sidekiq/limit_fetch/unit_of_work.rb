# frozen_string_literal: true

module Sidekiq
  class LimitFetch
    # Sidekiq's object to track a job being processed.
    # fetcher_class#retrieve_work is expected to return, so it must be a somewhat public API.
    class UnitOfWork < BasicFetch::UnitOfWork
      # Acknowledge completion of job
      def acknowledge
        Global::QueueSemaphore.new(config, queue_name).release
      end

      # Put the job back in Redis.
      def requeue
        acknowledge
        super
      end
    end
  end
end
