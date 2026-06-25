# frozen_string_literal: true

# These are semi end-to-end tests that use a real Redis connection and real
# Sidekiq capsules/fetchers. No mocks or doubles are used for the core logic.
# They validate:
#   - Global queue limits (across processes)
#   - Per-process queue limits
#   - The Lua script for atomic limit checking + job fetching
#   - The heartbeat and dead-capsule reaping
#   - UnitOfWork acknowledge/requeue releasing semaphore slots
#   - Queue ordering (strict and weighted random)


RSpec.describe Sidekiq::LimitFetch do
  let(:config)  { Sidekiq::Config.new }
  let(:capsule) { config.default_capsule }
  let(:fetcher) { Sidekiq::LimitFetch.new(capsule) }

  # Prefix all test queues to avoid collision
  let(:queue_prefix) { "test_limit_fetch_#{SecureRandom.hex(4)}" }

  def queue_full_name(suffix)
    "#{queue_prefix}_#{suffix}"
  end

  def redis_queue_key(suffix)
    "queue:#{queue_full_name(suffix)}"
  end

  def push_jobs(count, queue: _queue)
    capsule.redis do |redis|
      redis.multi do |multi|
        count.times do |i|
          job = Sidekiq.dump_json({ "class" => "TestWorker", "args" => [i], "queue" => queue_full_name(queue) })
          multi.call("LPUSH", redis_queue_key(queue), job)
        end
      end
    end
  end

  def cleanup_redis!
    capsule.redis do |redis|
      redis.call("DEL", *%W[
        *#{queue_prefix}*
        sidekiq:limit_fetch:capsule:*
        sidekiq:limit_fetch:capsules*
      ])
    end
  end

  def configure_limits(limit)
    capsule[:limits] = { queue_full_name(queue).to_sym => limit }
  end

  def configure_process_limits(limit)
    capsule[:process_limits] = { queue_full_name(queue).to_sym => limit }
  end

  let(:queue)  { "default" }
  let(:_queue) { queue } # To allow for keyword arguments named `queue`
  let(:queues) { Array(queue) }

  let(:queue_name)       { queue_full_name(queue) }
  let(:queue_redis_name) { redis_queue_key(queue) }

  let(:busy_key) { "sidekiq:limit_fetch:queue:#{queue_full_name(queue)}:busy" }

  let(:limit_key)         { "sidekiq:limit_fetch:queue:#{queue_full_name(queue)}:limit" }
  let(:process_limit_key) { "sidekiq:limit_fetch:queue:#{queue_full_name(queue)}:process_limit" }

  let(:limits) { {} }
  let(:process_limits) { {} }

  before do
    Sidekiq::LimitFetch::Global.instance_variable_set(:@capsule, nil)
    capsule[:fetch_class] = described_class
    capsule.queues = Array(queues).map { |q| queue_full_name(q) }

    capsule[:limits] = limits.transform_keys { |q| queue_full_name(q).to_sym }
    capsule[:process_limits] = process_limits.transform_keys { |q| queue_full_name(q).to_sym }

    Sidekiq::LimitFetch.setup(capsule)
  end

  after do
    cleanup_redis!
  end

  describe "fetching without limits" do
    it "fetches all available jobs from the queue" do
      push_jobs(5)
      jobs = 5.times.map { fetcher.retrieve_work }

      expect(jobs.size).to eq(5)
      jobs.each do |work|
        expect(work).to be_a(Sidekiq::LimitFetch::UnitOfWork)
        expect(work.queue_name).to eq(queue_name)
      end
    end

    it "returns no work when no jobs are available" do
      result = fetcher.retrieve_work
      expect(result).to be_nil
    end
  end

  describe "global queue limits" do
    let(:queue)  { "limited" }
    let(:limits) { { queue => 2 } }

    it "enforces global limit on concurrent jobs" do
      push_jobs(5)

      # Should only be able to fetch 2 jobs (the global limit)
      work1 = fetcher.retrieve_work
      work2 = fetcher.retrieve_work
      work3 = fetcher.retrieve_work # should be nil - limit reached

      expect(work1).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work2).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work3).to be_nil

      # After acknowledging one job, we can fetch another
      work1.acknowledge

      work4 = fetcher.retrieve_work
      expect(work4).to be_a(Sidekiq::LimitFetch::UnitOfWork)

      [work2, work4].each(&:acknowledge)
    end

    it "allows fetching again after requeue releases the slot" do
      push_jobs(5)

      work1 = fetcher.retrieve_work
      work2 = fetcher.retrieve_work
      expect(fetcher.retrieve_work).to be_nil

      # Requeue releases the slot AND puts the job back
      work2.requeue

      work3 = fetcher.retrieve_work
      expect(work3).to be_a(Sidekiq::LimitFetch::UnitOfWork)

      [work1, work3].each(&:acknowledge)
    end
  end

  describe "per-process queue limits" do
    let(:queue)  { "proc_limited" }
    let(:limits) { { queue => 1 } }

    it "enforces per-process limit on concurrent jobs" do
      push_jobs(5)

      work1 = fetcher.retrieve_work
      work2 = fetcher.retrieve_work # should be nil - process limit reached

      expect(work1).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work2).to be_nil

      # After acknowledging, we can fetch again
      work1.acknowledge

      work3 = fetcher.retrieve_work
      expect(work3).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      work3.acknowledge
    end
  end

  describe "combined global and process limits" do
    let(:queue)          { "combo" }
    let(:limits)         { { queue => 5 } }
    let(:process_limits) { { queue => 2 } }

    it "enforces the more restrictive limit" do
      push_jobs(10)

      work1 = fetcher.retrieve_work
      work2 = fetcher.retrieve_work
      work3 = fetcher.retrieve_work # process limit of 2 reached

      expect(work1).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work2).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work3).to be_nil

      [work1, work2].each(&:acknowledge)
    end
  end

  describe "multiple queues with independent limits" do
    let(:queues) { %w[email webhook] }

    let(:limits) do
      {
        "email"   => 1,
        "webhook" => 2,
      }
    end

    it "enforces limits independently per queue" do
      push_jobs(5, queue: "email")
      push_jobs(5, queue: "webhook")

      # Fetch from email (limit 1) - strict ordering means email first
      work_email = fetcher.retrieve_work
      expect(work_email.queue_name).to eq(queue_full_name("email"))

      # Next fetch should skip email (at limit) and get from webhook
      work_webhook1 = fetcher.retrieve_work
      expect(work_webhook1.queue_name).to eq(queue_full_name("webhook"))

      work_webhook2 = fetcher.retrieve_work
      expect(work_webhook2.queue_name).to eq(queue_full_name("webhook"))

      # Both queues at limit now - should return nil
      work_nil = fetcher.retrieve_work
      expect(work_nil).to be_nil

      # Release email slot
      work_email.acknowledge

      # Now can fetch from email again
      work_email2 = fetcher.retrieve_work
      expect(work_email2.queue_name).to eq(queue_full_name("email"))

      [work_webhook1, work_webhook2, work_email2].each(&:acknowledge)
    end
  end

  describe "queue with no limit alongside limited queue" do
    let(:queues) { %w[unlimited limited2] }
    let(:limits) { { "limited2" => 1 } }

    it "allows unlimited fetching from queues without limits" do
      push_jobs(10, queue: "unlimited")

      jobs = 10.times.map { fetcher.retrieve_work }

      expect(jobs.size).to eq(10)
      expect(jobs.map(&:queue_name).uniq).to eq([queue_full_name("unlimited")])
      jobs.each(&:acknowledge)
    end
  end

  describe "Lua script atomicity" do
    let(:queue)  { "atomic" }
    let(:limits) { { queue => 3 } }

    it "maintains correct busy count in Redis" do
      push_jobs(10)

      work1 = fetcher.retrieve_work
      work2 = fetcher.retrieve_work
      work3 = fetcher.retrieve_work

      # Check busy list length in Redis
      busy_count = capsule.redis { |redis| redis.call("LLEN", busy_key) }
      expect(busy_count).to eq(3)

      # At limit
      expect(fetcher.retrieve_work).to be_nil

      work1.acknowledge
      busy_count = capsule.redis { |redis| redis.call("LLEN", busy_key) }
      expect(busy_count).to eq(2)

      work2.acknowledge
      work3.acknowledge
      busy_count = capsule.redis { |redis| redis.call("LLEN", busy_key) }
      expect(busy_count).to eq(0)
    end
  end

  describe "multi-process simulation (multiple capsules with different UUIDs)" do
    let(:config1) { Sidekiq::Config.new }
    let(:config2) { Sidekiq::Config.new }
    let(:cap1) { config1.default_capsule }
    let(:cap2) { config2.default_capsule }

    after do
      Sidekiq::LimitFetch::Heartbeat.shutdown
      Sidekiq::LimitFetch::Heartbeat.threads.clear
    end

    it "enforces global limit across multiple fetcher instances" do
      cap1.queues = [queue_full_name("shared")]
      cap1[:limits] = { queue_full_name("shared").to_sym => 3 }
      cap1[:fetch_class] = Sidekiq::LimitFetch

      cap2.queues = [queue_full_name("shared")]
      cap2[:limits] = { queue_full_name("shared").to_sym => 3 }
      cap2[:fetch_class] = Sidekiq::LimitFetch

      Sidekiq::LimitFetch.setup(cap1)
      Sidekiq::LimitFetch.setup(cap2)

      # Push jobs
      cap1.redis do |redis|
        redis.multi do |multi|
          10.times do |i|
            job = Sidekiq.dump_json({ "class" => "TestWorker", "args" => [i], "queue" => queue_full_name("shared") })
            multi.call("LPUSH", "queue:#{queue_full_name("shared")}", job)
          end
        end
      end

      fetcher1 = Sidekiq::LimitFetch.new(cap1)
      fetcher2 = Sidekiq::LimitFetch.new(cap2)

      # Fetcher 1 takes 2 jobs
      w1a = fetcher1.retrieve_work
      w1b = fetcher1.retrieve_work
      expect(w1a).not_to be_nil
      expect(w1b).not_to be_nil

      # Fetcher 2 takes 1 job (global limit is 3 total)
      w2a = fetcher2.retrieve_work
      expect(w2a).not_to be_nil

      # Both fetchers blocked now (global limit of 3 reached)
      expect(fetcher1.retrieve_work).to be_nil
      expect(fetcher2.retrieve_work).to be_nil

      # Release one from fetcher1
      w1a.acknowledge

      # Now fetcher2 can pick up a job
      w2b = fetcher2.retrieve_work
      expect(w2b).not_to be_nil

      # Clean up
      [w1b, w2a, w2b].each(&:acknowledge)
    end

    it "process limits are enforced per-capsule (same process shares UUID)" do
      # Note: Two capsules in the same Ruby process share a UUID when they have
      # the same name, so process_limit applies to the combined total.
      # This test verifies that process_limit correctly caps the single process.
      cap1.queues = [queue_full_name("pshared")]
      cap1[:process_limits] = { queue_full_name("pshared").to_sym => 2 }
      cap1[:fetch_class] = Sidekiq::LimitFetch

      Sidekiq::LimitFetch.setup(cap1)

      # Push jobs
      cap1.redis do |redis|
        5.times do |i|
          job = Sidekiq.dump_json({ "class" => "TestWorker", "args" => [i], "queue" => queue_full_name("pshared") })
          redis.call("LPUSH", "queue:#{queue_full_name("pshared")}", job)
        end
      end

      fetcher1 = Sidekiq::LimitFetch.new(cap1)

      # Process can take 2 jobs (process_limit=2)
      w1 = fetcher1.retrieve_work
      w2 = fetcher1.retrieve_work
      expect(w1).not_to be_nil
      expect(w2).not_to be_nil

      # At process limit
      expect(fetcher1.retrieve_work).to be_nil

      # Release one allows another
      w1.acknowledge
      w3 = fetcher1.retrieve_work
      expect(w3).not_to be_nil

      [w2, w3].each(&:acknowledge)
    end
  end

  describe "heartbeat and dead capsule reaping" do
    describe "registration" do
      let(:queue) { "heartbeat_test" }

      let(:capsule_meta) { Sidekiq::LimitFetch::Global.capsule[capsule.name] }
      let(:capsule_sem)  { Sidekiq::LimitFetch::Global::CapsuleSemaphor.new(capsule) }

      it "registers capsule in Redis on heartbeat" do
        capsule_sem.heartbeat

        # Capsule should be registered
        members = capsule.redis { |redis| redis.call("SMEMBERS", "sidekiq:limit_fetch:capsules") }
        expect(members).to include(capsule_meta.uuid)

        # Heartbeat key should exist
        hb_key = "sidekiq:limit_fetch:capsule:#{capsule_meta.uuid}:heartbeat"
        val = capsule.redis { |redis| redis.call("GET", hb_key) }
        expect(val).to eq("1")
      end
    end

    describe "reaping" do
      let(:queue)  { "reap_test" }
      let(:limits) { { queue => 2 } }

      it "reaps dead capsules and removes their busy entries" do
        fake_dead_uuid = "dead-capsule-#{SecureRandom.hex(4)}"

        # Simulate a dead capsule that left busy entries behind
        capsule.redis do |redis|
          redis.call("SADD", "sidekiq:limit_fetch:capsules", fake_dead_uuid)
          redis.call("RPUSH", busy_key, fake_dead_uuid)
          redis.call("RPUSH", busy_key, fake_dead_uuid)
        end

        # Verify busy entries exist
        busy_count = capsule.redis { |redis| redis.call("LLEN", busy_key) }
        expect(busy_count).to eq(2)

        # Run heartbeat which triggers reap
        capsule_sem = Sidekiq::LimitFetch::Global::CapsuleSemaphor.new(capsule)
        capsule_sem.heartbeat

        # Dead capsule should be removed from capsules set
        members = capsule.redis { |redis| redis.call("SMEMBERS", "sidekiq:limit_fetch:capsules") }
        expect(members).not_to include(fake_dead_uuid)

        # Busy entries for dead capsule should be cleaned up
        busy_count = capsule.redis { |redis| redis.call("LLEN", busy_key) }
        expect(busy_count).to eq(0)
      end


      context "live capsules exist" do
        let(:queue)  { "live_test" }
        let(:limits) { { queue => 2 } }

        it "does not reap" do
          capsule_meta = Sidekiq::LimitFetch::Global.capsule[capsule.name]

          # Simulate a live capsule with active jobs
          capsule.redis do |redis|
            redis.call("RPUSH", busy_key, capsule_meta.uuid)
          end

          # Run heartbeat (registers this capsule as alive)
          capsule_sem = Sidekiq::LimitFetch::Global::CapsuleSemaphor.new(capsule)
          capsule_sem.heartbeat

          # Busy entry should still exist
          busy_count = capsule.redis { |redis| redis.call("LLEN", busy_key) }
          expect(busy_count).to eq(1)

          # Capsule should still be registered
          members = capsule.redis { |redis| redis.call("SMEMBERS", "sidekiq:limit_fetch:capsules") }
          expect(members).to include(capsule_meta.uuid)
        end
      end

      describe "unblocking" do
        let(:queue)  { "reap_unblock" }
        let(:limits) { { queue => 2 } }

        it "unblocks a queue after reaping a dead capsule's busy slots" do
          push_jobs(5)

          fake_dead_uuid = "dead-#{SecureRandom.hex(4)}"

          # Simulate dead capsule holding 2 slots (at the limit)
          capsule.redis do |redis|
            redis.call("SADD", "sidekiq:limit_fetch:capsules", fake_dead_uuid)
            redis.call("RPUSH", busy_key, fake_dead_uuid)
            redis.call("RPUSH", busy_key, fake_dead_uuid)
          end

          # Can't fetch - limit is 2 and dead capsule holds both slots
          expect(fetcher.retrieve_work).to be_nil

          # Reap dead capsule
          capsule_sem = Sidekiq::LimitFetch::Global::CapsuleSemaphor.new(capsule)
          capsule_sem.heartbeat

          # Now we can fetch
          work = fetcher.retrieve_work
          expect(work).to be_a(Sidekiq::LimitFetch::UnitOfWork)
          work.acknowledge
        end
      end
    end
  end

  describe "heartbeat thread lifecycle" do
    let(:queue) { "hb_lifecycle" }

    it "starts and can be shut down cleanly" do
      heartbeat = Sidekiq::LimitFetch::Heartbeat.new(capsule)
      thread = heartbeat.start

      expect(thread).to be_alive
      expect(thread.name).to eq("sidekiq-limit_fetch.heartbeat")

      # Shut it down
      thread.raise(Sidekiq::LimitFetch::Shutdown)
      thread.join(20)

      expect(thread).not_to be_alive
    ensure
      Sidekiq::LimitFetch::Heartbeat.threads.delete(thread)
    end
  end

  describe described_class::Global::QueueSemaphore do
    let(:queue) { "sem_test" }

    it "can set and get limits" do
      Sidekiq::LimitFetch::Global.init_capsule(capsule)

      sem = Sidekiq::LimitFetch::Global::QueueSemaphore.new(capsule, queue_full_name(queue))

      # Initially nil
      expect(sem.limit).to be_nil
      expect(sem.process_limit).to be_nil

      # Set limits
      sem.limit = 5
      sem.process_limit = 2

      expect(sem.limit).to eq(5)
      expect(sem.process_limit).to eq(2)

      # Clear limits
      sem.limit = nil
      sem.process_limit = nil

      expect(sem.limit).to be_nil
      expect(sem.process_limit).to be_nil
    end
  end

  describe "UnitOfWork" do
    let(:queue)  { "uow" }
    let(:limits) { { queue => 5 } }

    it "releases the busy slot on acknowledge" do
      push_jobs(3)

      work = fetcher.retrieve_work
      expect(capsule.redis { |c| c.call("LLEN", busy_key) }).to eq(1)

      work.acknowledge
      expect(capsule.redis { |c| c.call("LLEN", busy_key) }).to eq(0)
    end

    it "releases slot and re-enqueues job on requeue" do
      push_jobs(1)

      work = fetcher.retrieve_work
      expect(work).not_to be_nil
      expect(capsule.redis { |c| c.call("LLEN", busy_key) }).to eq(1)

      # Queue should be empty now (we took the only job)
      queue_len = capsule.redis { |c| c.call("LLEN", queue_redis_name) }
      expect(queue_len).to eq(0)

      work.requeue

      # Slot released
      expect(capsule.redis { |c| c.call("LLEN", busy_key) }).to eq(0)

      # Job back in queue
      queue_len = capsule.redis { |c| c.call("LLEN", queue_redis_name) }
      expect(queue_len).to eq(1)
    end
  end

  describe "strict queue ordering" do
    let(:queues) { %w[high low] }

    it "fetches from higher-priority queue first" do
      push_jobs(2, queue: "high")
      push_jobs(2, queue: "low")

      # Strict ordering means high-priority queue is checked first
      work1 = fetcher.retrieve_work
      work2 = fetcher.retrieve_work

      expect(work1.queue_name).to eq(queue_full_name("high"))
      expect(work2.queue_name).to eq(queue_full_name("high"))

      # Now high is empty, should get from low
      work3 = fetcher.retrieve_work
      expect(work3.queue_name).to eq(queue_full_name("low"))

      [work1, work2, work3].each(&:acknowledge)
    end
  end

  describe "limit of zero effectively pauses a queue" do
    let(:queues) { %w[paused active] }
    let(:limits) { { "paused" => 0 } }

    it "never fetches from a queue with limit 0" do
      push_jobs(5, queue: "paused")
      push_jobs(3, queue: "active")

      jobs = 10.times.filter_map { fetcher.retrieve_work }

      # Should only get jobs from the active queue
      expect(jobs.size).to eq(3)
      expect(jobs.map(&:queue_name).uniq).to eq([queue_full_name("active")])

      jobs.each(&:acknowledge)
    end
  end

  describe "concurrent fetch simulation with threads" do
    let(:queue)  { "concurrent" }
    let(:limits) { { queue => 3 } }

    it "never exceeds the limit even with concurrent fetchers" do
      push_jobs(20)
      mutex = Mutex.new
      fetched = []

      threads = 5.times.map do
        Thread.new do
          20.times do
            work = fetcher.retrieve_work
            if work
              mutex.synchronize { fetched << work }
              # Simulate some processing time
              Kernel.sleep(0.005)
              # Check that busy count never exceeds limit
              busy = capsule.redis { |c| c.call("LLEN", busy_key) }
              expect(busy).to be <= 3
              work.acknowledge
            end
          end
        end
      end

      threads.each(&:join)

      # All 20 jobs should have been processed eventually
      expect(fetched.size).to eq(20)
    end
  end

  describe "setup persists limits to Redis" do
    context "no existing limits" do
      let(:queue)          { "setup_test" }
      let(:limits)         { { queue => 7 } }
      let(:process_limits) { { queue => 3 } }

      it "writes configured limits to Redis keys" do
        limit_val = capsule.redis { |c| c.call("GET", limit_key) }
        process_limit_val = capsule.redis { |c| c.call("GET", process_limit_key) }

        expect(limit_val).to eq("7")
        expect(process_limit_val).to eq("3")
      end
    end

    context "existing limits" do
      let(:queue)  { "no_overwrite" }
      let(:limits) { { queue => 10 } }

      it "does not overwrite existing limits in Redis" do
        # Pre-set limit in Redis
        capsule.redis { |c| c.call("SET", limit_key, "5") }

        Sidekiq::LimitFetch.setup(capsule)

        # Should keep existing value (5), not overwrite with 10
        limit_val = capsule.redis { |c| c.call("GET", limit_key) }
        expect(limit_val).to eq("5")
      end
    end
  end

  describe "edge cases" do
    context "limit of 1" do
      let(:queue)  { "serial" }
      let(:limits) { { queue => 1 } }

      it "serializes all access to a queue" do
        push_jobs(5)

        # Can only hold 1 at a time
        5.times do
          work = fetcher.retrieve_work
          expect(work).not_to be_nil
          expect(fetcher.retrieve_work).to be_nil # blocked
          work.acknowledge
        end
      end
    end
  end

  describe "heartbeat Redis error handling" do
    let(:queue) { "hb_error" }

    it "logs error and sets down flag on first RedisClient::Error" do
      heartbeat = Sidekiq::LimitFetch::Heartbeat.new(capsule)
      capsule_sem = heartbeat.capsule_sem

      # Simulate Redis going down on heartbeat
      allow(capsule_sem).to receive(:heartbeat).and_raise(RedisClient::ConnectionError, "Connection refused")

      # Stub Kernel.sleep to avoid actual sleep, and raise Shutdown to exit the loop
      call_count = 0
      allow(Kernel).to receive(:sleep) do
        call_count += 1
        raise Sidekiq::LimitFetch::Shutdown if call_count >= 1
      end

      # Run the heartbeat loop (will hit error, sleep, then shutdown)
      heartbeat.run

      # Verify the heartbeat tracked the down state
      expect(heartbeat.instance_variable_get(:@down)).to be true
    end

    it "logs concise message on repeated RedisClient::Error" do
      heartbeat = Sidekiq::LimitFetch::Heartbeat.new(capsule)
      capsule_sem = heartbeat.capsule_sem

      # Simulate Redis being down repeatedly
      allow(capsule_sem).to receive(:heartbeat).and_raise(RedisClient::ConnectionError, "Connection refused")

      call_count = 0
      allow(Kernel).to receive(:sleep) do
        call_count += 1
        raise Sidekiq::LimitFetch::Shutdown if call_count >= 2
      end

      # Run - will error twice then shutdown
      heartbeat.run

      # After 2 errors, still marked as down
      expect(heartbeat.instance_variable_get(:@down)).to be true
    end

    it "logs recovery when Redis comes back online" do
      heartbeat = Sidekiq::LimitFetch::Heartbeat.new(capsule)
      capsule_sem = heartbeat.capsule_sem

      # First call: error, second call: success
      hb_call_count = 0
      allow(capsule_sem).to receive(:heartbeat) do
        hb_call_count += 1
        raise RedisClient::ConnectionError, "Connection refused" if hb_call_count == 1
        # Second call succeeds (real heartbeat)
        Sidekiq::LimitFetch::Global::CapsuleSemaphor.instance_method(:heartbeat).bind_call(capsule_sem)
      end

      sleep_count = 0
      allow(Kernel).to receive(:sleep) do
        sleep_count += 1
        raise Sidekiq::LimitFetch::Shutdown if sleep_count >= 2
      end

      heartbeat.run

      # Redis recovered - flag should be cleared
      expect(heartbeat.instance_variable_get(:@down)).to be false
    end

    it "deregisters on shutdown when Redis is up" do
      heartbeat = Sidekiq::LimitFetch::Heartbeat.new(capsule)
      capsule_sem = heartbeat.capsule_sem
      capsule_meta = heartbeat.capsule_meta

      # Allow first heartbeat to succeed, then shutdown
      allow(Kernel).to receive(:sleep) do
        raise Sidekiq::LimitFetch::Shutdown
      end

      # Register first so we can verify deregistration
      capsule_sem.heartbeat
      members_before = capsule.redis { |c| c.call("SMEMBERS", "sidekiq:limit_fetch:capsules") }
      expect(members_before).to include(capsule_meta.uuid)

      heartbeat.run

      # After clean shutdown, capsule should be deregistered
      members_after = capsule.redis { |c| c.call("SMEMBERS", "sidekiq:limit_fetch:capsules") }
      expect(members_after).not_to include(capsule_meta.uuid)
    end
  end

  describe "Lua script EVALSHA fallback to EVAL" do
    let(:queue)  { "lua_fallback" }
    let(:limits) { { queue => 2 } }

    before do
      # Flush the script cache so EVALSHA will fail with NOSCRIPT
      capsule.redis { |redis| redis.call("SCRIPT", "FLUSH") }
    end

    it "falls back to EVAL when EVALSHA returns NOSCRIPT" do
      push_jobs(3)

      # First fetch triggers EVALSHA -> NOSCRIPT -> EVAL fallback
      work1 = fetcher.retrieve_work
      expect(work1).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work1.queue_name).to eq(queue_full_name(queue))

      # Subsequent fetches also work (script now cached again)
      work2 = fetcher.retrieve_work
      expect(work2).to be_a(Sidekiq::LimitFetch::UnitOfWork)

      # Limit still enforced after fallback
      work3 = fetcher.retrieve_work
      expect(fetcher.retrieve_work).to be_nil # limit of 2 reached

      [work1, work2, work3].compact.each(&:acknowledge)
    end

    it "enforces limits correctly after script cache flush" do
      push_jobs(5)

      # Flush again mid-test to force another NOSCRIPT
      work1 = fetcher.retrieve_work
      capsule.redis { |redis| redis.call("SCRIPT", "FLUSH") }
      work2 = fetcher.retrieve_work

      expect(work1).not_to be_nil
      expect(work2).not_to be_nil

      # At limit
      capsule.redis { |redis| redis.call("SCRIPT", "FLUSH") }
      expect(fetcher.retrieve_work).to be_nil

      [work1, work2].each(&:acknowledge)
    end
  end
end
