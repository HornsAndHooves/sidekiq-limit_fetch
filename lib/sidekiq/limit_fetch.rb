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

    # Time between heartbeats.
    HEARTBEAT_PERIOD = 15

    # Uses ~0% CPU but jobs fill up all threads in one process before being distributed to others
    #
    # Consider a queue with a global limit of 1 and two processes (A & B) that are able to process jobs from the queue.
    # The first worker thread to be waiting for jobs from a queue keeps a "reservation" on it which prevents others from looking at it.
    # This effectively means that only this worker is always going to get jobs from that queue.
    #
    # Another issue, consider:
    #
    #   email   limit = 2
    #   payment limit = 2
    #
    #   Process A:
    #     Thread A1
    #     Thread A2
    #
    #   Process B:
    #     Thread B1
    #     Thread B2
    #
    # A1 reserves: [email slot1, email slot2, payment slot1, payment slot2]
    # A2 reserves: [email slot1, email slot2, payment slot1, payment slot2]
    # B1 reserves: []
    # B2 reserves: []
    # -> Two jobs come in to email queue, and two jobs come in to payment queue at the same time
    # -> A1 takes an email job and A2 takes an email job
    # -> B1 & B2 need to wait until their sleep timeout before processing the payment jobs

    # POLL_RANGE = Range.new(0.050, 0.100) # ~3.4%
    # POLL_RANGE = Range.new(0.100, 0.200) # 1.3%
    # POLL_RANGE = Range.new(0.200, 0.300) # 0.8%
    # POLL_RANGE = Range.new(0.400, 0.500) # ~0.5%
    # POLL_RANGE = Range.new(1.000, 1.200) # ~0.1%

    # STRATEGY = 'WAIT'

    # Better job distribution but uses more CPU
    #
    # Workers will sleep for a random number in this range when no jobs are found.
    # This means when there is no queue backlog the maximum time to pick up a job will be within this range.
    # Queues with a backlog will continue to process jobs immediately.

    STRATEGY = "POLL"

    # Ranges with median CPU utilization on macOS with concurrency of 5:
    POLL_RANGE = Range.new(0.050, 0.100) # ~2.7%
    # POLL_RANGE = Range.new(0.100, 0.200) # 1.3%
    # POLL_RANGE = Range.new(0.200, 0.300) # 0.8%
    # POLL_RANGE = Range.new(0.400, 0.500) # ~0.5%
    # POLL_RANGE = Range.new(1.000, 1.200) # ~0.1%

    # Raise this exception to heartbeat threads when Sidekiq sends the shutdown hook.
    class Shutdown < StandardError; end

    # @param capsule [Sidekiq::Capsule]
    def self.setup(capsule)
      capsule_meta = Global.init_capsule(capsule)

      limits         = capsule[:limits] || {}
      process_limits = capsule[:process_limits] || {}

      capsule_meta.queue_set.each do |queue_name|
        queue = Global::QueueSemaphore.new(capsule, queue_name)

        # Apply process limit
        if queue.process_limit.nil?
          queue.process_limit = process_limits[queue_name.to_sym]
        end

        # Apply global limit
        if queue.limit.nil?
          queue.limit = limits[queue_name.to_sym]
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

    # @return [UnitOfWork, nil]
    def retrieve_work
      ordered_queues = queues_cmd # BasicFetch method handles randomization or strict ordering

      case STRATEGY
      when "POLL" then run_poll(ordered_queues)

      # :nocov: This will probably be removed
      when "WAIT" then run_wait(ordered_queues)
      # :nocov:
      end
    end

    # Lua:
    #   * If limits permit us to take a job, try to take a job (RPOP) from the Sidekiq queue
    #   * If a job is found:
    #     * Add the current process UUID to the internal "busy" list for that queue
    #     * Return Sidekiq job string to the client
    def run_poll(ordered_queues)
      job_queue, job_str = selector.limit_fetch(ordered_queues)
      return UnitOfWork.new(job_queue, job_str, capsule) if job_str

      Kernel.sleep(poll_interval)
      nil
    end

    # Lua:
    #   * If limits permit us to take a job:
    #     * Add the current process UUID to the internal "busy" list for that queue
    #     * Append to return array
    # :nocov: This will probably be removed
    def run_wait(ordered_queues)
      reserved_queues = selector.limit_fetch(ordered_queues)

      # Same behavior as BasicFetch
      if reserved_queues.empty?
        Kernel.sleep(poll_interval)
        return
      end

      job_queue, job_str = capsule.redis do |redis|
        timeout = poll_interval
        redis.blocking_call(timeout, "BRPOP", *reserved_queues, timeout)
      end

      # Release locks on queues we were waiting on
      capsule.redis do |redis|
        redis.multi do |multi|
          reserved_queues.each do |queue|
            next if job_queue == queue
            multi.lrem("sidekiq:limit_fetch:#{queue}:busy", 1, Global.capsule[capsule.name].uuid)
          end
        end
      end

      UnitOfWork.new(job_queue, job_str, capsule) if job_str
    end
    # :nocov:

    # @return [Float]
    def poll_interval
      Random.rand(POLL_RANGE)
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
