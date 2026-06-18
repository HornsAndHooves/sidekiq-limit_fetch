# frozen_string_literal: true

require "sidekiq"
require "sidekiq/fetch"

# :nodoc:
module Sidekiq
  # A Sidekiq fetch strategy that enforces global and per-process concurrency
  # limits on queues. Replaces {Sidekiq::BasicFetch} and uses a Lua script to
  # atomically check limits and pop jobs from Redis.
  class LimitFetch < BasicFetch
    require_relative "limit_fetch/unit_of_work"
    require_relative "limit_fetch/global"
    require_relative "limit_fetch/global/semaphore"
    require_relative "limit_fetch/global/selector"
    require_relative "limit_fetch/heartbeat"

    # Raise this exception to heartbeat threads when Sidekiq sends the shutdown hook.
    class Shutdown < StandardError; end

    def self.configuration
      @configuration ||= {
        # Workers will sleep for a random number in this range when no jobs are found.
        # This means when there is no queue backlog the maximum time to pick up a job will be within this range.
        # Queues with a backlog will continue to process jobs immediately.
        poll_range: Range.new(0.400, 0.500),

        # Time between heartbeats.
        heartbeat_period: 15,
      }
    end

    # Ranges with median CPU utilization on macOS with concurrency of 5:
    # poll_range = Range.new(0.050, 0.100) # ~2.7%
    # poll_range = Range.new(0.100, 0.200) # 1.3%
    # poll_range = Range.new(0.200, 0.300) # 0.8%
    # poll_range = Range.new(0.400, 0.500) # ~0.5%
    # poll_range = Range.new(1.000, 1.200) # ~0.1%

    # @param capsule [Sidekiq::Capsule]
    def self.setup(capsule)
      capsule_meta = Global.init_capsule(capsule)

      limits         = capsule[:limits] || {}
      process_limits = capsule[:process_limits] || {}

      capsule_meta.queue_set.each do |queue_name|
        queue = Global::QueueSemaphore.new(capsule, queue_name)
        queue_sym = queue_name.to_sym

        # Apply process limit
        if queue.process_limit.nil?
          queue.process_limit = process_limits[queue_sym]
        end

        # Apply global limit
        if queue.limit.nil?
          queue.limit = limits[queue_sym]
        end
      end

      LimitFetch::Heartbeat.new(capsule).start
    end

    attr_reader :capsule

    # @param capsule [Sidekiq::Capsule]
    def initialize(capsule)
      @capsule = capsule
      super
    end

    # Lua:
    #   * If limits permit us to take a job, try to take a job (RPOP) from the Sidekiq queue
    #   * If a job is found:
    #     * Add the current process UUID to the internal "busy" list for that queue
    #     * Return Sidekiq job string to the client
    #
    # @return [UnitOfWork, nil]
    def retrieve_work
      ordered_queues = queues_cmd # BasicFetch method handles randomization or strict ordering

      job_queue, job_str = selector.limit_fetch(ordered_queues)
      return UnitOfWork.new(job_queue, job_str, capsule) if job_str

      Kernel.sleep(poll_interval)
      nil
    end

    # @return [Float]
    def poll_interval
      Random.rand(self.class.configuration[:poll_range])
    end

    # @return [Global::Selector]
    def selector
      @selector ||= Global::Selector.new(capsule)
    end
  end
end

Sidekiq.configure_server do |config|
  # :nocov:
  config.default_capsule do |capsule|
    capsule.config[:fetch_class] = Sidekiq::LimitFetch

    # This will raise an exception if Redis is down, and the app won't boot
    Sidekiq::LimitFetch.setup(capsule)
  end

  config.on(:shutdown) do
    Sidekiq::LimitFetch::Heartbeat.shutdown
  end
  # :nocov:
end
