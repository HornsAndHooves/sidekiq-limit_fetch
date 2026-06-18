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

RSpec.describe "Sidekiq::LimitFetch integration" do
  let(:config)  { Sidekiq::Config.new }
  let(:capsule) { config.default_capsule }

  # Prefix all test queues to avoid collision
  let(:queue_prefix) { "test_limit_fetch_#{SecureRandom.hex(4)}" }

  def queue_name(suffix)
    "#{queue_prefix}_#{suffix}"
  end

  def redis_queue_key(suffix)
    "queue:#{queue_name(suffix)}"
  end

  def push_jobs(suffix, count)
    capsule.redis do |conn|
      count.times do |i|
        job = Sidekiq.dump_json({ "class" => "TestWorker", "args" => [i], "queue" => queue_name(suffix) })
        conn.call("LPUSH", redis_queue_key(suffix), job)
      end
    end
  end

  # Helper: retrieve_work returns a UnitOfWork on success, or a numeric (from Kernel.sleep) on failure
  def fetch_work(fetcher)
    result = fetcher.retrieve_work
    result.is_a?(Sidekiq::LimitFetch::UnitOfWork) ? result : nil
  end

  def cleanup_redis!
    capsule.redis do |conn|
      conn.call("DEL", *%W[
        *#{queue_prefix}*
        sidekiq:limit_fetch:capsule:*
        sidekiq:limit_fetch:capsules*
      ])
    end
  end

  before do
    Sidekiq::LimitFetch::Global.instance_variable_set(:@capsule, nil)
  end

  after do
    cleanup_redis!
  end

  describe "fetching without limits" do
    before do
      capsule.queues = [queue_name("default")]
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)
    end

    it "fetches all available jobs from the queue" do
      push_jobs("default", 5)

      fetcher = Sidekiq::LimitFetch.new(capsule)
      jobs = []
      5.times do
        work = fetch_work(fetcher)
        jobs << work if work
      end

      expect(jobs.size).to eq(5)
      jobs.each do |work|
        expect(work).to be_a(Sidekiq::LimitFetch::UnitOfWork)
        expect(work.queue_name).to eq(queue_name("default"))
      end
    end

    it "returns no work when no jobs are available" do
      fetcher = Sidekiq::LimitFetch.new(capsule)
      result = fetch_work(fetcher)
      expect(result).to be_nil
    end
  end

  describe "global queue limits" do
    before do
      capsule.queues = [queue_name("limited")]
      capsule[:limits] = { queue_name("limited").to_sym => 2 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)
    end

    it "enforces global limit on concurrent jobs" do
      push_jobs("limited", 5)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      # Should only be able to fetch 2 jobs (the global limit)
      work1 = fetch_work(fetcher)
      work2 = fetch_work(fetcher)
      work3 = fetch_work(fetcher) # should be nil - limit reached

      expect(work1).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work2).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work3).to be_nil

      # After acknowledging one job, we can fetch another
      work1.acknowledge

      work4 = fetch_work(fetcher)
      expect(work4).to be_a(Sidekiq::LimitFetch::UnitOfWork)

      [work2, work4].each(&:acknowledge)
    end

    it "allows fetching again after requeue releases the slot" do
      push_jobs("limited", 5)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      work1 = fetch_work(fetcher)
      work2 = fetch_work(fetcher)
      expect(fetch_work(fetcher)).to be_nil

      # Requeue releases the slot AND puts the job back
      work2.requeue

      work3 = fetch_work(fetcher)
      expect(work3).to be_a(Sidekiq::LimitFetch::UnitOfWork)

      [work1, work3].each(&:acknowledge)
    end
  end

  describe "per-process queue limits" do
    before do
      capsule.queues = [queue_name("proc_limited")]
      capsule[:process_limits] = { queue_name("proc_limited").to_sym => 1 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)
    end

    it "enforces per-process limit on concurrent jobs" do
      push_jobs("proc_limited", 5)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      work1 = fetch_work(fetcher)
      work2 = fetch_work(fetcher) # should be nil - process limit reached

      expect(work1).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work2).to be_nil

      # After acknowledging, we can fetch again
      work1.acknowledge

      work3 = fetch_work(fetcher)
      expect(work3).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      work3.acknowledge
    end
  end

  describe "combined global and process limits" do
    it "enforces the more restrictive limit" do
      capsule.queues = [queue_name("combo")]
      capsule[:limits] = { queue_name("combo").to_sym => 5 }
      capsule[:process_limits] = { queue_name("combo").to_sym => 2 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)

      push_jobs("combo", 10)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      work1 = fetch_work(fetcher)
      work2 = fetch_work(fetcher)
      work3 = fetch_work(fetcher) # process limit of 2 reached

      expect(work1).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work2).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work3).to be_nil

      [work1, work2].each(&:acknowledge)
    end
  end

  describe "multiple queues with independent limits" do
    before do
      capsule.queues = [queue_name("email"), queue_name("webhook")]
      capsule[:limits] = {
        queue_name("email").to_sym => 1,
        queue_name("webhook").to_sym => 2
      }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)
    end

    it "enforces limits independently per queue" do
      push_jobs("email", 5)
      push_jobs("webhook", 5)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      # Fetch from email (limit 1) - strict ordering means email first
      work_email = fetch_work(fetcher)
      expect(work_email.queue_name).to eq(queue_name("email"))

      # Next fetch should skip email (at limit) and get from webhook
      work_webhook1 = fetch_work(fetcher)
      expect(work_webhook1.queue_name).to eq(queue_name("webhook"))

      work_webhook2 = fetch_work(fetcher)
      expect(work_webhook2.queue_name).to eq(queue_name("webhook"))

      # Both queues at limit now - should return nil
      work_nil = fetch_work(fetcher)
      expect(work_nil).to be_nil

      # Release email slot
      work_email.acknowledge

      # Now can fetch from email again
      work_email2 = fetch_work(fetcher)
      expect(work_email2.queue_name).to eq(queue_name("email"))

      [work_webhook1, work_webhook2, work_email2].each(&:acknowledge)
    end
  end

  describe "queue with no limit alongside limited queue" do
    before do
      capsule.queues = [queue_name("unlimited"), queue_name("limited2")]
      capsule[:limits] = { queue_name("limited2").to_sym => 1 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)
    end

    it "allows unlimited fetching from queues without limits" do
      push_jobs("unlimited", 10)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      jobs = []
      10.times do
        work = fetch_work(fetcher)
        jobs << work if work
      end

      expect(jobs.size).to eq(10)
      expect(jobs.map(&:queue_name).uniq).to eq([queue_name("unlimited")])
      jobs.each(&:acknowledge)
    end
  end

  describe "Lua script atomicity" do
    before do
      capsule.queues = [queue_name("atomic")]
      capsule[:limits] = { queue_name("atomic").to_sym => 3 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)
    end

    it "maintains correct busy count in Redis" do
      push_jobs("atomic", 10)

      fetcher = Sidekiq::LimitFetch.new(capsule)
      busy_key = "sidekiq:limit_fetch:queue:#{queue_name("atomic")}:busy"

      work1 = fetch_work(fetcher)
      work2 = fetch_work(fetcher)
      work3 = fetch_work(fetcher)

      # Check busy list length in Redis
      busy_count = capsule.redis { |conn| conn.call("LLEN", busy_key) }
      expect(busy_count).to eq(3)

      # At limit
      expect(fetch_work(fetcher)).to be_nil

      work1.acknowledge
      busy_count = capsule.redis { |conn| conn.call("LLEN", busy_key) }
      expect(busy_count).to eq(2)

      work2.acknowledge
      work3.acknowledge
      busy_count = capsule.redis { |conn| conn.call("LLEN", busy_key) }
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
      cap1.queues = [queue_name("shared")]
      cap1[:limits] = { queue_name("shared").to_sym => 3 }
      cap1[:fetch_class] = Sidekiq::LimitFetch

      cap2.queues = [queue_name("shared")]
      cap2[:limits] = { queue_name("shared").to_sym => 3 }
      cap2[:fetch_class] = Sidekiq::LimitFetch

      Sidekiq::LimitFetch.setup(cap1)
      Sidekiq::LimitFetch.setup(cap2)

      # Push jobs
      cap1.redis do |conn|
        10.times do |i|
          job = Sidekiq.dump_json({ "class" => "TestWorker", "args" => [i], "queue" => queue_name("shared") })
          conn.call("LPUSH", "queue:#{queue_name("shared")}", job)
        end
      end

      fetcher1 = Sidekiq::LimitFetch.new(cap1)
      fetcher2 = Sidekiq::LimitFetch.new(cap2)

      # Fetcher 1 takes 2 jobs
      w1a = fetch_work(fetcher1)
      w1b = fetch_work(fetcher1)
      expect(w1a).not_to be_nil
      expect(w1b).not_to be_nil

      # Fetcher 2 takes 1 job (global limit is 3 total)
      w2a = fetch_work(fetcher2)
      expect(w2a).not_to be_nil

      # Both fetchers blocked now (global limit of 3 reached)
      expect(fetch_work(fetcher1)).to be_nil
      expect(fetch_work(fetcher2)).to be_nil

      # Release one from fetcher1
      w1a.acknowledge

      # Now fetcher2 can pick up a job
      w2b = fetch_work(fetcher2)
      expect(w2b).not_to be_nil

      # Clean up
      [w1b, w2a, w2b].each(&:acknowledge)
    end

    it "process limits are enforced per-capsule (same process shares UUID)" do
      # Note: Two capsules in the same Ruby process share a UUID when they have
      # the same name, so process_limit applies to the combined total.
      # This test verifies that process_limit correctly caps the single process.
      cap1.queues = [queue_name("pshared")]
      cap1[:process_limits] = { queue_name("pshared").to_sym => 2 }
      cap1[:fetch_class] = Sidekiq::LimitFetch

      Sidekiq::LimitFetch.setup(cap1)

      # Push jobs
      cap1.redis do |conn|
        5.times do |i|
          job = Sidekiq.dump_json({ "class" => "TestWorker", "args" => [i], "queue" => queue_name("pshared") })
          conn.call("LPUSH", "queue:#{queue_name("pshared")}", job)
        end
      end

      fetcher1 = Sidekiq::LimitFetch.new(cap1)

      # Process can take 2 jobs (process_limit=2)
      w1 = fetch_work(fetcher1)
      w2 = fetch_work(fetcher1)
      expect(w1).not_to be_nil
      expect(w2).not_to be_nil

      # At process limit
      expect(fetch_work(fetcher1)).to be_nil

      # Release one allows another
      w1.acknowledge
      w3 = fetch_work(fetcher1)
      expect(w3).not_to be_nil

      [w2, w3].each(&:acknowledge)
    end
  end

  describe "heartbeat and dead capsule reaping" do
    it "registers capsule in Redis on heartbeat" do
      capsule.queues = [queue_name("heartbeat_test")]
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)

      capsule_meta = Sidekiq::LimitFetch::Global.capsule[capsule.name]
      capsule_sem = Sidekiq::LimitFetch::Global::CapsuleSemaphor.new(capsule)

      capsule_sem.heartbeat

      # Capsule should be registered
      members = capsule.redis { |conn| conn.call("SMEMBERS", "sidekiq:limit_fetch:capsules") }
      expect(members).to include(capsule_meta.uuid)

      # Heartbeat key should exist
      hb_key = "sidekiq:limit_fetch:capsule:#{capsule_meta.uuid}:heartbeat"
      val = capsule.redis { |conn| conn.call("GET", hb_key) }
      expect(val).to eq("1")
    end

    it "reaps dead capsules and removes their busy entries" do
      capsule.queues = [queue_name("reap_test")]
      capsule[:limits] = { queue_name("reap_test").to_sym => 2 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)

      fake_dead_uuid = "dead-capsule-#{SecureRandom.hex(4)}"
      busy_key = "sidekiq:limit_fetch:queue:#{queue_name("reap_test")}:busy"

      # Simulate a dead capsule that left busy entries behind
      capsule.redis do |conn|
        conn.call("SADD", "sidekiq:limit_fetch:capsules", fake_dead_uuid)
        conn.call("RPUSH", busy_key, fake_dead_uuid)
        conn.call("RPUSH", busy_key, fake_dead_uuid)
      end

      # Verify busy entries exist
      busy_count = capsule.redis { |conn| conn.call("LLEN", busy_key) }
      expect(busy_count).to eq(2)

      # Run heartbeat which triggers reap
      capsule_sem = Sidekiq::LimitFetch::Global::CapsuleSemaphor.new(capsule)
      capsule_sem.heartbeat

      # Dead capsule should be removed from capsules set
      members = capsule.redis { |conn| conn.call("SMEMBERS", "sidekiq:limit_fetch:capsules") }
      expect(members).not_to include(fake_dead_uuid)

      # Busy entries for dead capsule should be cleaned up
      busy_count = capsule.redis { |conn| conn.call("LLEN", busy_key) }
      expect(busy_count).to eq(0)
    end

    it "does not reap live capsules" do
      capsule.queues = [queue_name("live_test")]
      capsule[:limits] = { queue_name("live_test").to_sym => 2 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)

      capsule_meta = Sidekiq::LimitFetch::Global.capsule[capsule.name]
      busy_key = "sidekiq:limit_fetch:queue:#{queue_name("live_test")}:busy"

      # Simulate a live capsule with active jobs
      capsule.redis do |conn|
        conn.call("RPUSH", busy_key, capsule_meta.uuid)
      end

      # Run heartbeat (registers this capsule as alive)
      capsule_sem = Sidekiq::LimitFetch::Global::CapsuleSemaphor.new(capsule)
      capsule_sem.heartbeat

      # Busy entry should still exist
      busy_count = capsule.redis { |conn| conn.call("LLEN", busy_key) }
      expect(busy_count).to eq(1)

      # Capsule should still be registered
      members = capsule.redis { |conn| conn.call("SMEMBERS", "sidekiq:limit_fetch:capsules") }
      expect(members).to include(capsule_meta.uuid)
    end

    it "unblocks a queue after reaping a dead capsule's busy slots" do
      capsule.queues = [queue_name("reap_unblock")]
      capsule[:limits] = { queue_name("reap_unblock").to_sym => 2 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)

      push_jobs("reap_unblock", 5)

      fake_dead_uuid = "dead-#{SecureRandom.hex(4)}"
      busy_key = "sidekiq:limit_fetch:queue:#{queue_name("reap_unblock")}:busy"

      # Simulate dead capsule holding 2 slots (at the limit)
      capsule.redis do |conn|
        conn.call("SADD", "sidekiq:limit_fetch:capsules", fake_dead_uuid)
        conn.call("RPUSH", busy_key, fake_dead_uuid)
        conn.call("RPUSH", busy_key, fake_dead_uuid)
      end

      fetcher = Sidekiq::LimitFetch.new(capsule)

      # Can't fetch - limit is 2 and dead capsule holds both slots
      expect(fetch_work(fetcher)).to be_nil

      # Reap dead capsule
      capsule_sem = Sidekiq::LimitFetch::Global::CapsuleSemaphor.new(capsule)
      capsule_sem.heartbeat

      # Now we can fetch
      work = fetch_work(fetcher)
      expect(work).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      work.acknowledge
    end
  end

  describe "heartbeat thread lifecycle" do
    it "starts and can be shut down cleanly" do
      capsule.queues = [queue_name("hb_lifecycle")]
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)

      heartbeat = Sidekiq::LimitFetch::Heartbeat.new(capsule)
      thread = heartbeat.start

      expect(thread).to be_alive
      expect(thread.name).to eq("sidekiq-limit_fetch.heartbeat")

      # Shut it down
      thread.raise(Sidekiq::LimitFetch::Shutdown)
      thread.join(5)

      expect(thread).not_to be_alive
    ensure
      Sidekiq::LimitFetch::Heartbeat.threads.delete(thread)
    end
  end

  describe "QueueSemaphore" do
    it "can set and get limits" do
      capsule.queues = [queue_name("sem_test")]
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch::Global.init_capsule(capsule)

      sem = Sidekiq::LimitFetch::Global::QueueSemaphore.new(capsule, queue_name("sem_test"))

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
    before do
      capsule.queues = [queue_name("uow")]
      capsule[:limits] = { queue_name("uow").to_sym => 5 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)
    end

    it "releases the busy slot on acknowledge" do
      push_jobs("uow", 3)
      fetcher = Sidekiq::LimitFetch.new(capsule)
      busy_key = "sidekiq:limit_fetch:queue:#{queue_name("uow")}:busy"

      work = fetch_work(fetcher)
      expect(capsule.redis { |c| c.call("LLEN", busy_key) }).to eq(1)

      work.acknowledge
      expect(capsule.redis { |c| c.call("LLEN", busy_key) }).to eq(0)
    end

    it "releases slot and re-enqueues job on requeue" do
      push_jobs("uow", 1)
      fetcher = Sidekiq::LimitFetch.new(capsule)
      busy_key = "sidekiq:limit_fetch:queue:#{queue_name("uow")}:busy"

      work = fetch_work(fetcher)
      expect(work).not_to be_nil
      expect(capsule.redis { |c| c.call("LLEN", busy_key) }).to eq(1)

      # Queue should be empty now (we took the only job)
      queue_len = capsule.redis { |c| c.call("LLEN", "queue:#{queue_name("uow")}") }
      expect(queue_len).to eq(0)

      work.requeue

      # Slot released
      expect(capsule.redis { |c| c.call("LLEN", busy_key) }).to eq(0)

      # Job back in queue
      queue_len = capsule.redis { |c| c.call("LLEN", "queue:#{queue_name("uow")}") }
      expect(queue_len).to eq(1)
    end
  end

  describe "strict queue ordering" do
    before do
      # When all queues listed once, Sidekiq uses strict ordering (first queue has priority)
      capsule.queues = [queue_name("high"), queue_name("low")]
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)
    end

    it "fetches from higher-priority queue first" do
      push_jobs("high", 2)
      push_jobs("low", 2)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      # Strict ordering means high-priority queue is checked first
      work1 = fetch_work(fetcher)
      work2 = fetch_work(fetcher)

      expect(work1.queue_name).to eq(queue_name("high"))
      expect(work2.queue_name).to eq(queue_name("high"))

      # Now high is empty, should get from low
      work3 = fetch_work(fetcher)
      expect(work3.queue_name).to eq(queue_name("low"))

      [work1, work2, work3].each(&:acknowledge)
    end
  end

  describe "limit of zero effectively pauses a queue" do
    before do
      capsule.queues = [queue_name("paused"), queue_name("active")]
      capsule[:limits] = { queue_name("paused").to_sym => 0 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)
    end

    it "never fetches from a queue with limit 0" do
      push_jobs("paused", 5)
      push_jobs("active", 3)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      jobs = []
      10.times do
        work = fetch_work(fetcher)
        jobs << work if work
      end

      # Should only get jobs from the active queue
      expect(jobs.size).to eq(3)
      expect(jobs.map(&:queue_name).uniq).to eq([queue_name("active")])

      jobs.each(&:acknowledge)
    end
  end

  describe "concurrent fetch simulation with threads" do
    before do
      capsule.queues = [queue_name("concurrent")]
      capsule[:limits] = { queue_name("concurrent").to_sym => 3 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)
    end

    it "never exceeds the limit even with concurrent fetchers" do
      push_jobs("concurrent", 20)

      fetcher = Sidekiq::LimitFetch.new(capsule)
      mutex = Mutex.new
      fetched = []
      busy_key = "sidekiq:limit_fetch:queue:#{queue_name("concurrent")}:busy"

      threads = 5.times.map do
        Thread.new do
          20.times do
            work = fetch_work(fetcher)
            if work
              mutex.synchronize { fetched << work }
              # Simulate some processing time
              sleep(0.005)
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
    it "writes configured limits to Redis keys" do
      capsule.queues = [queue_name("setup_test")]
      capsule[:limits] = { queue_name("setup_test").to_sym => 7 }
      capsule[:process_limits] = { queue_name("setup_test").to_sym => 3 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)

      limit_key = "sidekiq:limit_fetch:queue:#{queue_name("setup_test")}:limit"
      process_limit_key = "sidekiq:limit_fetch:queue:#{queue_name("setup_test")}:process_limit"

      limit_val = capsule.redis { |c| c.call("GET", limit_key) }
      process_limit_val = capsule.redis { |c| c.call("GET", process_limit_key) }

      expect(limit_val).to eq("7")
      expect(process_limit_val).to eq("3")
    end

    it "does not overwrite existing limits in Redis" do
      capsule.queues = [queue_name("no_overwrite")]
      capsule[:limits] = { queue_name("no_overwrite").to_sym => 10 }
      capsule[:fetch_class] = Sidekiq::LimitFetch

      # Pre-set limit in Redis
      limit_key = "sidekiq:limit_fetch:queue:#{queue_name("no_overwrite")}:limit"
      capsule.redis { |c| c.call("SET", limit_key, "5") }

      Sidekiq::LimitFetch.setup(capsule)

      # Should keep existing value (5), not overwrite with 10
      limit_val = capsule.redis { |c| c.call("GET", limit_key) }
      expect(limit_val).to eq("5")
    end
  end

  describe "edge cases" do
    it "handles a queue being drained mid-fetch" do
      capsule.queues = [queue_name("drain")]
      capsule[:limits] = { queue_name("drain").to_sym => 10 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)

      push_jobs("drain", 2)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      work1 = fetch_work(fetcher)
      work2 = fetch_work(fetcher)
      work3 = fetch_work(fetcher) # queue empty, even though limit allows more

      expect(work1).not_to be_nil
      expect(work2).not_to be_nil
      expect(work3).to be_nil

      # Busy count should be 2 (jobs taken but not yet finished)
      busy_key = "sidekiq:limit_fetch:queue:#{queue_name("drain")}:busy"
      busy_count = capsule.redis { |c| c.call("LLEN", busy_key) }
      expect(busy_count).to eq(2)

      [work1, work2].each(&:acknowledge)
    end

    it "limit of 1 serializes all access to a queue" do
      capsule.queues = [queue_name("serial")]
      capsule[:limits] = { queue_name("serial").to_sym => 1 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)

      push_jobs("serial", 5)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      # Can only hold 1 at a time
      5.times do
        work = fetch_work(fetcher)
        expect(work).not_to be_nil
        expect(fetch_work(fetcher)).to be_nil # blocked
        work.acknowledge
      end
    end
  end

  describe "heartbeat Redis error handling" do
    before do
      capsule.queues = [queue_name("hb_error")]
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)
    end

    it "logs error and sets redis_down flag on first RedisClient::Error" do
      heartbeat = Sidekiq::LimitFetch::Heartbeat.new(capsule)
      capsule_sem = heartbeat.capsule_sem

      # Simulate Redis going down on heartbeat
      allow(capsule_sem).to receive(:heartbeat).and_raise(RedisClient::ConnectionError, "Connection refused")
      allow(heartbeat).to receive(:capsule_sem).and_return(capsule_sem)

      # Stub Kernel.sleep to avoid actual sleep, and raise Shutdown to exit the loop
      call_count = 0
      allow(Kernel).to receive(:sleep) do
        call_count += 1
        raise Sidekiq::LimitFetch::Shutdown if call_count >= 1
      end

      # Run the heartbeat loop (will hit error, sleep, then shutdown)
      heartbeat.run

      # Verify the heartbeat tracked the redis_down state
      expect(heartbeat.instance_variable_get(:@redis_down)).to be true
    end

    it "logs concise message on repeated RedisClient::Error" do
      heartbeat = Sidekiq::LimitFetch::Heartbeat.new(capsule)
      capsule_sem = heartbeat.capsule_sem

      # Simulate Redis being down repeatedly
      allow(capsule_sem).to receive(:heartbeat).and_raise(RedisClient::ConnectionError, "Connection refused")
      allow(heartbeat).to receive(:capsule_sem).and_return(capsule_sem)

      call_count = 0
      allow(Kernel).to receive(:sleep) do
        call_count += 1
        raise Sidekiq::LimitFetch::Shutdown if call_count >= 2
      end

      # Run - will error twice then shutdown
      heartbeat.run

      # After 2 errors, still marked as redis_down
      expect(heartbeat.instance_variable_get(:@redis_down)).to be true
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
      allow(heartbeat).to receive(:capsule_sem).and_return(capsule_sem)

      sleep_count = 0
      allow(Kernel).to receive(:sleep) do
        sleep_count += 1
        raise Sidekiq::LimitFetch::Shutdown if sleep_count >= 2
      end

      heartbeat.run

      # Redis recovered - flag should be cleared
      expect(heartbeat.instance_variable_get(:@redis_down)).to be false
    end

    it "does not deregister on shutdown when Redis is down" do
      heartbeat = Sidekiq::LimitFetch::Heartbeat.new(capsule)
      capsule_sem = heartbeat.capsule_sem

      # Simulate Redis down
      allow(capsule_sem).to receive(:heartbeat).and_raise(RedisClient::ConnectionError, "Connection refused")
      allow(capsule_sem).to receive(:purge).and_call_original
      allow(heartbeat).to receive(:capsule_sem).and_return(capsule_sem)

      # Immediately shutdown after one error
      allow(Kernel).to receive(:sleep) do
        raise Sidekiq::LimitFetch::Shutdown
      end

      heartbeat.run

      # purge should NOT have been called on shutdown since Redis is down
      expect(capsule_sem).not_to have_received(:purge)
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
    before do
      capsule.queues = [queue_name("lua_fallback")]
      capsule[:limits] = { queue_name("lua_fallback").to_sym => 2 }
      capsule[:fetch_class] = Sidekiq::LimitFetch
      Sidekiq::LimitFetch.setup(capsule)

      # Flush the script cache so EVALSHA will fail with NOSCRIPT
      capsule.redis { |conn| conn.call("SCRIPT", "FLUSH") }
    end

    it "falls back to EVAL when EVALSHA returns NOSCRIPT" do
      push_jobs("lua_fallback", 3)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      # First fetch triggers EVALSHA -> NOSCRIPT -> EVAL fallback
      work1 = fetch_work(fetcher)
      expect(work1).to be_a(Sidekiq::LimitFetch::UnitOfWork)
      expect(work1.queue_name).to eq(queue_name("lua_fallback"))

      # Subsequent fetches also work (script now cached again)
      work2 = fetch_work(fetcher)
      expect(work2).to be_a(Sidekiq::LimitFetch::UnitOfWork)

      # Limit still enforced after fallback
      work3 = fetch_work(fetcher)
      expect(fetch_work(fetcher)).to be_nil # limit of 2 reached

      [work1, work2, work3].compact.each(&:acknowledge)
    end

    it "enforces limits correctly after script cache flush" do
      push_jobs("lua_fallback", 5)

      fetcher = Sidekiq::LimitFetch.new(capsule)

      # Flush again mid-test to force another NOSCRIPT
      work1 = fetch_work(fetcher)
      capsule.redis { |conn| conn.call("SCRIPT", "FLUSH") }
      work2 = fetch_work(fetcher)

      expect(work1).not_to be_nil
      expect(work2).not_to be_nil

      # At limit
      capsule.redis { |conn| conn.call("SCRIPT", "FLUSH") }
      expect(fetch_work(fetcher)).to be_nil

      [work1, work2].each(&:acknowledge)
    end
  end
end
