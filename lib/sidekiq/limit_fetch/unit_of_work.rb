# frozen_string_literal: true

module Sidekiq
  module LimitFetch
    class UnitOfWork < BasicFetch::UnitOfWork
      def initialize(...)
        super
        redis_retryable { Queue[queue_name].increase_busy }
      end

      def acknowledge
        redis_retryable { Queue[queue_name].decrease_busy }
        redis_retryable { Queue[queue_name].release }
      end

      def requeue
        super
        acknowledge
      end

      private

      def redis_retryable(&block)
        Sidekiq::LimitFetch.redis_retryable(&block)
      end
    end
  end
end
