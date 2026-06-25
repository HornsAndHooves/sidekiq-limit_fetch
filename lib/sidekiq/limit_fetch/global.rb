# frozen_string_literal: true

module Sidekiq
  class LimitFetch
    # Namespace for global state management including capsule metadata,
    # Redis-backed semaphores, and the Lua-based queue selector.
    class Global

      # Internal tracking of capsule information (namely UUID).
      CapsuleMetadata = Struct.new(:name, :uuid, :queue_set, keyword_init: true)

      # Holds capsule metadata (will only have 1 capsule in most Sidekiq setups).
      #
      # @return [Hash{String => CapsuleMetadata}]
      def self.capsule
        @capsule ||= {}
      end

      # @param cap [Sidekiq::Capsule]
      #
      # @return [CapsuleMetadata]
      def self.init_capsule(cap)
        capsule[cap.name] = CapsuleMetadata.new(
          name:      cap.name,
          uuid:      SecureRandom.uuid,
          queue_set: cap.queues.to_set,
        )
      end
    end
  end
end
