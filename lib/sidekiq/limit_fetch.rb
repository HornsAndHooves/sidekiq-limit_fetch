# frozen_string_literal: true

require 'forwardable'
require 'sidekiq'
require 'sidekiq/manager'
require 'sidekiq/api'

module Sidekiq
  module LimitFetch
    autoload :UnitOfWork, 'sidekiq/limit_fetch/unit_of_work'

    require_relative 'limit_fetch/instances'
    require_relative 'limit_fetch/queues'
    require_relative 'limit_fetch/global/semaphore'
    require_relative 'limit_fetch/global/selector'
    require_relative 'limit_fetch/global/monitor'
    require_relative 'extensions/queue'
    require_relative 'extensions/manager'

    TIMEOUT = Sidekiq::BasicFetch::TIMEOUT

    extend self

    RedisBaseConnectionError = RedisClient::ConnectionError
    RedisCommandError = RedisClient::CommandError

    def new(_)
      self
    end

    def retrieve_work
      queue, job = redis_brpop(Queues.acquire)
      Queues.release_except(queue)
      UnitOfWork.new(queue, job) if job
    end

    def config
      Sidekiq.options
    end

    def bulk_requeue(*args)
      Sidekiq::BasicFetch.new(Sidekiq.default_configuration.default_capsule).bulk_requeue(*args)
    end

    def redis_retryable
      yield
    rescue RedisBaseConnectionError
      sleep TIMEOUT
      retry
    rescue RedisCommandError => e
      # If Redis was restarted and is still loading its snapshot,
      # then we should treat this as a temporary connection error too.
      raise unless e.message =~ /^LOADING/

      sleep TIMEOUT
      retry
    end

    private

    # rubocop:disable Metrics/MethodLength
    def redis_brpop(queues)
      if queues.empty?
        sleep TIMEOUT  # there are no queues to handle, so lets sleep
        []             # and return nothing
      else
        redis_retryable do
          Sidekiq.redis do |it|
            it.blocking_call(false, 'brpop', *queues, TIMEOUT)
          end
        end
      end
    end
    # rubocop:enable Metrics/MethodLength
  end
end
