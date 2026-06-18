# HornsAndHooves-sidekiq-limit_fetch

`HornsAndHooves-sidekiq-limit_fetch` is a Sidekiq fetch strategy for Sidekiq 8+ that limits how many jobs may be fetched from each queue at the same time.

It is useful when a queue represents a constrained resource: a third-party API, a database-heavy workflow, a single-tenant workload, a pool of external workers, or any job class where normal Sidekiq concurrency is too broad a control. Instead of reducing Sidekiq's entire process concurrency, you can keep Sidekiq busy while allowing selected queues to run only a fixed number of jobs concurrently.

This fork was completely rewritten for v5.0.0. The current implementation is intentionally focused on queue concurrency limiting, with Redis-backed state, an atomic Lua fetch path, and heartbeat-based cleanup of stale process state.

## High-level overview

Sidekiq normally decides which queue to fetch from, pops a job, and then runs it. This gem replaces Sidekiq's server-side fetch class with `Sidekiq::LimitFetch`. Before a job is popped from Redis, `LimitFetch` checks the queue's configured limits. If taking the job would exceed those limits, the queue is skipped for that fetch attempt and another eligible queue may be tried.

The user can configure:

- **Global queue limits** with `:limits` — the maximum number of jobs from a queue that may be in progress across all Sidekiq processes using the same Redis.
- **Per-process queue limits** with `:process_limits` — the maximum number of jobs from a queue that a single Sidekiq process/capsule may have in progress.
- **Normal Sidekiq queue order and weights** with `:queues` — this gem uses Sidekiq's own queue ordering behavior and applies limits while fetching from those queues.
- **Idle polling behavior** with `Sidekiq::LimitFetch.configuration[:poll_range]` — how long a fetcher sleeps when no eligible job is found.
- **Heartbeat cadence** with `Sidekiq::LimitFetch.configuration[:heartbeat_period]` — how often each process records liveness and reaps stale concurrency slots left behind by dead processes.

A queue with no configured limit behaves like a normal Sidekiq queue. A limit of `0` effectively prevents jobs from being fetched from that queue.

## Requirements

- Ruby `>= 2.7`
- Sidekiq `>= 8`
- Redis supported by your Sidekiq version

This version does **not** support Sidekiq versions below 8. If you need Sidekiq 6 or 7 support, use an older/original version of `sidekiq-limit_fetch`.

## Installation

Add the gem to your application's `Gemfile`:

```ruby
gem "HornsAndHooves-sidekiq-limit_fetch", require: "sidekiq-limit_fetch"
```

Then install it:

```bash
bundle install
```

If you are not using Bundler auto-require, require it explicitly in your Sidekiq server boot path:

```ruby
require "sidekiq-limit_fetch"
```

The gem installs itself during `Sidekiq.configure_server` by setting the default capsule's `fetch_class` to `Sidekiq::LimitFetch` and starting the heartbeat thread.

If your application uses additional Sidekiq capsules, configure each capsule that should use limited fetching:

```ruby
Sidekiq.configure_server do |config|
  config.capsule("limited") do |capsule|
    capsule.queues = %w[webhooks exports]
    capsule.config[:fetch_class] = Sidekiq::LimitFetch
    capsule[:limits] = { webhooks: 2, exports: 1 }

    Sidekiq::LimitFetch.setup(capsule)
  end
end
```

Most applications use only the default capsule and do not need this extra setup.

## Basic configuration

Configure limits in your Sidekiq YAML file.

```yaml
concurrency: 20
queues:
  - critical
  - mailers
  - webhooks

limits:
  mailers: 5
  webhooks: 2

process_limits:
  webhooks: 1
```

With this configuration:

- `critical` has no limit and is fetched normally.
- `mailers` may have at most 5 jobs in progress across the whole Sidekiq cluster.
- `webhooks` may have at most 2 jobs in progress across the whole Sidekiq cluster.
- Each Sidekiq process/capsule may run at most 1 `webhooks` job at a time.

The keys under `:limits` and `:process_limits` should match Sidekiq queue names.

## Global queue limits

A global queue limit caps concurrent jobs for a queue across all Sidekiq processes that share Redis.

```yaml
queues:
 - default
 - api_calls

limits:
  api_calls: 3
```

In this example, the Sidekiq cluster may process at most 3 `api_calls` jobs at the same time, no matter how many Sidekiq processes are running. Other queues continue to run according to normal Sidekiq behavior and their own limits.

Use global limits when the resource you are protecting is shared by the entire cluster, such as an external API account, a database, or a tenant-specific queue.

## Per-process limits

A per-process limit caps concurrent jobs for a queue within a single Sidekiq process/capsule.

```yaml
queues:
 - images
 - exports

process_limits:
  exports: 1
```

In this example, every Sidekiq process may run at most one `exports` job at a time. If you run 4 Sidekiq processes, the total cluster concurrency for `exports` could be up to 4 unless you also set a global limit.

Per-process limits are useful when each Ruby process has a local bottleneck, such as memory pressure, local file handles, or a connection pool.

## Combining global and per-process limits

You can configure both limit types for the same queue:

```yaml
queues:
 - reports

limits:
  reports: 10

process_limits:
  reports: 2
```

This means:

- no more than 10 `reports` jobs may run across the cluster; and
- no one process may run more than 2 `reports` jobs at a time.

Both checks must pass before a job is fetched.

## Queue ordering and weights

`Sidekiq::LimitFetch` inherits from Sidekiq's `BasicFetch` and uses Sidekiq's queue command generation. That means your normal `:queues` configuration still controls the order in which queues are considered.

Strict ordering works as expected:

```yaml
queues:
  - critical
  - default
  - low
```

Weighted queues also use Sidekiq's behavior:

```yaml
queues:
  - [critical, 5]
  - [default, 2]
  - [low, 1]
```

Limits are applied after Sidekiq determines the queue order for a fetch attempt. If a higher-priority queue is currently at its limit, it is skipped and a lower-priority eligible queue can be fetched.

## Pausing a queue with a zero limit

Set a queue's global limit to `0` to prevent new jobs from being fetched from that queue:

```yaml
limits:
  maintenance_queue: 0
```

Existing jobs already being processed are not interrupted. The queue simply stops yielding new jobs while the limit remains zero.

## Runtime limit changes

Limits are stored in Redis under the `sidekiq:limit_fetch:*` namespace. During setup, YAML-configured limits are written only when no value already exists in Redis, so an existing runtime value is preserved across process restarts.

The low-level semaphore API can be used from server-side code to change limits dynamically:

```ruby
capsule = Sidekiq.default_configuration.default_capsule
queue = Sidekiq::LimitFetch::Global::QueueSemaphore.new(capsule, "webhooks")

queue.limit = 5          # set global limit
queue.process_limit = 1  # set per-process limit

queue.limit = nil          # remove global limit
queue.process_limit = nil  # remove per-process limit
```

This is a lower-level API than the old `Sidekiq::Queue[...]` extensions from earlier versions. The v5 rewrite does not currently provide queue pausing, blocking, dynamic queues, or `Sidekiq::Queue['name'].limit = ...` helpers.

## Tuning idle polling

When no eligible job is found, a fetcher sleeps for a random duration in `Sidekiq::LimitFetch.configuration[:poll_range]`.

Default:

```ruby
Sidekiq::LimitFetch.configuration[:poll_range]
# => 0.4..0.5
```

You can tune this during Sidekiq server boot:

```ruby
Sidekiq.configure_server do |_config|
  Sidekiq::LimitFetch.configuration[:poll_range] = 0.2..0.3
end
```

Smaller values reduce the maximum delay before a newly available job is noticed, but increase idle Redis polling and CPU usage. Larger values reduce idle overhead, but can make quiet queues feel less responsive. Queues with a backlog continue to process immediately as long as limits allow work to be fetched.

## Tuning heartbeat cleanup

Each Sidekiq process/capsule gets a UUID and periodically writes a heartbeat key to Redis. Heartbeats serve two purposes:

1. They register the process/capsule as alive.
2. They allow other heartbeat cycles to detect dead processes and remove stale busy slots.

Default:

```ruby
Sidekiq::LimitFetch.configuration[:heartbeat_period]
# => 15
```

The heartbeat key expires after four heartbeat periods. With the default setting, stale process state is eligible for cleanup after roughly 60 seconds.

You can tune the heartbeat period during server boot:

```ruby
Sidekiq.configure_server do |_config|
  Sidekiq::LimitFetch.configuration[:heartbeat_period] = 10
end
```

Shorter periods clean up dead process slots faster but perform more Redis heartbeat work. Longer periods reduce heartbeat overhead but allow stale busy slots to remain longer after an unclean shutdown.

## Limitations and compatibility

> [!WARNING]
> This gem replaces Sidekiq's fetch strategy. It is incompatible with other Sidekiq extensions that also replace or substantially modify the fetch strategy.

Known incompatible or likely incompatible features include:

- Sidekiq Pro `super_fetch`
- `sidekiq-rate-limiter`
- any plugin that rewrites Sidekiq's fetch class or assumes `Sidekiq::BasicFetch` semantics directly

The v5 rewrite is focused on queue concurrency limits. Features from older versions that are **not** currently implemented include:

- queue pause/unpause helper methods;
- blocking queue mode;
- dynamic queues not declared in Sidekiq configuration;
- `Sidekiq::Queue[...]` limit helper methods; and
- queue busy-count query helpers.

## Operational notes

- The gem stores internal state in Redis keys prefixed with `sidekiq:limit_fetch:`.
- If Redis is flushed, any runtime limits and heartbeat/busy state are removed. Restart Sidekiq processes and reapply any runtime-only limit changes.
- Clean shutdown deregisters the process/capsule and removes its busy slots.
- Unclean shutdowns are handled by heartbeat expiry and reaping.
- During a Redis outage, heartbeat errors are logged and the heartbeat thread keeps retrying. If Redis is down during shutdown, the process does not attempt deregistration.

## In-depth technical overview

### Fetch strategy integration

The core class is `Sidekiq::LimitFetch`, a subclass of `Sidekiq::BasicFetch`. On Sidekiq server boot, the gem configures the default capsule:

```ruby
capsule.config[:fetch_class] = Sidekiq::LimitFetch
Sidekiq::LimitFetch.setup(capsule)
```

`Sidekiq::LimitFetch#retrieve_work` asks Sidekiq for the ordered list of Redis queue names via `queues_cmd`. This preserves Sidekiq's strict and weighted queue behavior. It then delegates to `Sidekiq::LimitFetch::Global::Selector`, which runs the Lua selector script against Redis.

If the script returns a queue and job payload, `retrieve_work` wraps them in `Sidekiq::LimitFetch::UnitOfWork`. If no queue is eligible or no job is available, the fetcher sleeps for a random interval from `poll_range` and returns `nil`.

### Redis data model

For a Sidekiq queue named `webhooks`, the gem uses keys like:

```text
sidekiq:limit_fetch:queue:webhooks:limit
sidekiq:limit_fetch:queue:webhooks:process_limit
sidekiq:limit_fetch:queue:webhooks:busy
```

Where:

- `:limit` stores the global queue concurrency limit.
- `:process_limit` stores the per-process/capsule queue concurrency limit.
- `:busy` is a Redis list containing one entry per in-progress job, where each entry is the UUID of the process/capsule that fetched that job.

Capsule liveness is tracked with:

```text
sidekiq:limit_fetch:capsules
sidekiq:limit_fetch:capsule:<uuid>:heartbeat
```

The `:capsules` set stores known active capsule UUIDs. Each heartbeat key is set with an expiration of `heartbeat_period * 4` seconds.

### Atomic selection with Lua

The file `lib/sidekiq/limit_fetch/global/limit_fetch.lua` contains the Redis Lua script that performs the critical fetch operation atomically.

For each queue in the Sidekiq-provided order, the script:

1. Reads the queue's global limit and process limit.
2. Counts the process's current busy slots for that queue when a process limit exists.
3. Counts total busy slots for that queue when a global limit exists.
4. Skips the queue if either limit would be exceeded.
5. Runs `RPOP` on the Sidekiq queue if the queue is eligible.
6. If a job is found, appends the capsule UUID to the queue's busy list with `RPUSH` and returns the queue/job pair.

Because the check, pop, and busy-slot increment happen inside one Redis script, concurrent Sidekiq threads and processes cannot overrun the configured limits between separate Redis commands.

The selector first tries `EVALSHA` with the script SHA. If Redis reports `NOSCRIPT`, it falls back to `EVAL` with the full script body.

### Unit of work lifecycle

`Sidekiq::LimitFetch::UnitOfWork` subclasses Sidekiq's `BasicFetch::UnitOfWork`.

When Sidekiq acknowledges a completed job, `UnitOfWork#acknowledge` removes one occurrence of the capsule UUID from that queue's busy list. When Sidekiq requeues a job, `UnitOfWork#requeue` first releases the busy slot and then delegates to Sidekiq's normal requeue behavior.

This ensures that both successful completion and requeue paths return concurrency capacity to the queue.

### Heartbeats and stale slot reaping

`Sidekiq::LimitFetch::Heartbeat` starts a thread named `sidekiq-limit_fetch.heartbeat` for each configured capsule. On each heartbeat interval, it:

1. Sets the capsule heartbeat key with an expiration.
2. Adds the capsule UUID to the global `sidekiq:limit_fetch:capsules` set.
3. Scans registered capsule UUIDs for missing heartbeat keys.
4. Removes dead UUIDs from the capsule set.
5. Removes dead UUIDs from each known queue's busy list.

This design allows the cluster to recover from a process that died while holding busy slots. Until the dead process is reaped, its slots still count toward queue limits; after reaping, other processes can use that capacity.

### Setup behavior

`Sidekiq::LimitFetch.setup(capsule)` initializes metadata for the capsule, applies configured `:limits` and `:process_limits` for the capsule's configured queues, and starts the heartbeat.

Configured limits are only written if the corresponding Redis key is not already set. This allows runtime changes stored in Redis to survive a Sidekiq restart instead of being overwritten by static YAML on every boot.

### Design goals of v5.0.0

The v5 rewrite favors a smaller and more predictable implementation:

- atomic limit enforcement in Redis;
- compatibility with Sidekiq 8's fetch/capsule model;
- independent global and per-process concurrency controls;
- resilience to unclean process exits through heartbeat reaping; and
- preserving Sidekiq's own queue-ordering semantics rather than reimplementing scheduling.
