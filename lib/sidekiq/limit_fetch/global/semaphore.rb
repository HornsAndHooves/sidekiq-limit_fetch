# frozen_string_literal: true

module Sidekiq
  class LimitFetch
    class Global
      # Base class for Redis-backed semaphores that track queue concurrency.
      # Provides common initialization and Redis access for subclasses.
      class SemaphoreBase
        # Redis key prefix for all limit_fetch data.
        PREFIX = "sidekiq:limit_fetch"

        # @param capsule [Sidekiq::Capsule]
        def initialize(capsule)
          @capsule = capsule
          @capsule_meta = Global.capsule[capsule.name]
          @capsule_uuid = @capsule_meta.uuid
        end

        private

        # Delegates to the capsule's Redis connection pool.
        #
        # @yield [conn] Redis connection
        def redis(...)
          @capsule.redis(...)
        end
      end

      # Manages concurrency semaphore state for an individual queue.
      # Stores global limits, per-process limits, and the busy list in Redis.
      class QueueSemaphore < SemaphoreBase
        def initialize(capsule, queue_name)
          super(capsule)
          queue_name = queue_name.delete_prefix("queue:")
          @prefix    = "#{PREFIX}:queue:#{queue_name}"
        end

        # Returns the global concurrency limit for this queue.
        #
        # @return [Integer, nil]
        def limit
          redis { |conn| conn.get("#{@prefix}:limit") }&.to_i
        end

        # Sets the global concurrency limit for this queue.
        #
        # @param value [Integer, nil]
        def limit=(value)
          if value
            redis { |conn| conn.set("#{@prefix}:limit", value) }
          else
            redis { |conn| conn.del("#{@prefix}:limit") }
          end
        end

        # Returns the per-process concurrency limit for this queue.
        #
        # @return [Integer, nil]
        def process_limit
          redis { |conn| conn.get("#{@prefix}:process_limit") }&.to_i
        end

        # Sets the per-process concurrency limit for this queue.
        #
        # @param value [Integer, nil]
        def process_limit=(value)
          if value
            redis { |conn| conn.set("#{@prefix}:process_limit", value) }
          else
            redis { |conn| conn.del("#{@prefix}:process_limit") }
          end
        end

        # Releases one busy slot for this capsule on the queue.
        # Called when a job finishes processing (acknowledge) or is requeued.
        def release
          redis { |conn| conn.lrem("#{@prefix}:busy", 1, @capsule_uuid) }
        end
      end

      # Manages capsule-level heartbeat registration and dead-capsule reaping.
      # Each Sidekiq process registers itself and periodically heartbeats to
      # signal liveness. Stale capsules are reaped and their busy slots freed.
      class CapsuleSemaphor < SemaphoreBase
        def initialize(capsule)
          super
          @prefix = "#{PREFIX}:capsule:#{@capsule_uuid}"
        end

        # Set a heartbeat key in Redis and ensure the capsule UUID is in the active set in Redis.
        # Heartbeat takes ~2.5ms on macOS
        def heartbeat
          redis do |conn|
            conn.multi do |multi|
              multi.set("#{@prefix}:heartbeat", "1", "ex", LimitFetch::HEARTBEAT_PERIOD * 4)
              multi.sadd("#{SemaphoreBase::PREFIX}:capsules", @capsule_uuid)
            end
          end

          reap
        end

        # Returns all registered capsule UUIDs.
        #
        # @return [Array<String>]
        def list
          redis { |conn| conn.smembers("#{SemaphoreBase::PREFIX}:capsules") }
        end

        # Finds and purges dead capsules. Called on every heartbeat.
        def reap
          dead_capsules = list_dead
          purge(dead_capsules)
        end

        # Identifies dead capsules by checking for missing heartbeat keys.
        #
        # @return [Array<String>]
        def list_dead
          uuids = list
          return uuids if uuids.empty?
          heartbeat_keys = uuids.map { |uuid| "#{SemaphoreBase::PREFIX}:capsule:#{uuid}:heartbeat" }
          heartbeat_statuses = redis { |conn| conn.mget(*heartbeat_keys) }
          uuids.zip(heartbeat_statuses).filter_map { |uuid, status| uuid if status != "1" }
        end

        # Removes dead capsules from the registry and cleans up their busy slots
        # across all queues managed by this capsule.
        #
        # @param uuids [Array<String>]
        def purge(uuids)
          return if uuids.empty?
          redis do |conn|
            conn.multi do |multi|
              multi.srem("#{SemaphoreBase::PREFIX}:capsules", *uuids)
              uuids.each do |uuid|
                multi.del("#{SemaphoreBase::PREFIX}:capsule:#{uuid}:heartbeat")
                @capsule_meta.queue_set.each do |queue|
                  multi.lrem("#{SemaphoreBase::PREFIX}:queue:#{queue}:busy", 0, uuid)
                end
              end
            end
          end
        end
      end
    end
  end
end
