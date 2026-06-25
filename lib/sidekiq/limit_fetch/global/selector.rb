# frozen_string_literal: true

module Sidekiq
  class LimitFetch
    class Global
      # Executes the limit_fetch Lua script against Redis to atomically check
      # concurrency limits and pop jobs from queues.
      class Selector

        # Attributes to send to the Lua script.
        ATTRS = %w[process_limit limit busy].freeze

        attr_reader :capsule

        # @param capsule [Sidekiq::Capsule]
        def initialize(capsule)
          @capsule = capsule
        end

        # @param queue_rnames [Array<String>] queue names in Redis (ex: 'queue:email', 'queue:payments', etc.)
        #
        # @return [String, nil] Sidekiq job string (if job is found)
        def limit_fetch(queue_rnames)
          keys = queue_rnames.each_with_object([]) do |queue, result|
            result << queue
            ATTRS.each { |attr| result << "sidekiq:limit_fetch:#{queue}:#{attr}" }
          end

          capsule_uuid = Global.capsule[capsule.name].uuid
          redis_eval(keys.size, *keys, capsule_uuid)
        end

        # Run the `limit_fetch.lua` script.
        def redis_eval(...)
          capsule.redis do |redis|
            sha = self.class.redis_script_sha
            redis.call("EVALSHA", sha, ...)
          rescue RedisClient::CommandError => e
            raise unless e.message.include?("NOSCRIPT")

            script = self.class.redis_script
            redis.call("EVAL", script, ...)
          end
        end

        # @return [String]
        def self.redis_script_sha
          @redis_script_sha ||= OpenSSL::Digest::SHA1.hexdigest(redis_script).freeze
        end

        # @return [String]
        def self.redis_script
          @redis_script ||= File.read("#{__dir__}/limit_fetch.lua").freeze
        end
      end
    end
  end
end
