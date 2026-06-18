-- For each queue:
--   * Check limit constraints to see whether this process can take a job from the queue
--   [POLL]:
--     * If limits permit us to take a job, try to take a job (RPOP) from the Sidekiq queue
--     * If a job is found:
--       * Add the current process UUID to the internal "busy" list for that queue
--       * Return Sidekiq job string to the client
--   [WAIT]:
--     * If limits permit us to take a job:
--       * Add the process UUID to the internal "busy" list for that queue

local capsule_uuid = ARGV[1]
local global_locks, process_locks
local found_job
local queue_config
local process_limit_key, global_limit_key, busy_key
local global_limit, process_limit

-- Unpack keys to table structure:
--   {
--     "queue:email" => [
--       "sidekiq:limit_fetch:queue:email:process_limit",
--       "sidekiq:limit_fetch:queue:email:limit",
--       "sidekiq:limit_fetch:queue:email:busy",
--     ]
--   }
local queues = {} -- Preserves order
local queue_configs = {}
local current_queue_name
for _, key in ipairs(KEYS) do
  if key:find("queue:", 1, true) == 1 then
    queues[#queues+1] = key
    current_queue_name = key
    queue_configs[current_queue_name] = {}
  else
    queue_config = queue_configs[current_queue_name]
    queue_config[#queue_config+1] = key
  end
end

for _, queue in ipairs(queues) do
  queue_config      = queue_configs[queue]
  process_limit_key = queue_config[1]
  global_limit_key  = queue_config[2]
  busy_key          = queue_config[3]

  global_limit, process_limit =
    unpack(redis.call("MGET",
      global_limit_key,
      process_limit_key
    ))

  global_limit  = tonumber(global_limit)
  process_limit = tonumber(process_limit)

  if process_limit then
    process_locks = #(redis.call("LPOS", busy_key, capsule_uuid, "COUNT", 0))
  end

  if not process_limit or process_limit > process_locks then
    if global_limit then
      global_locks = redis.call("LLEN", busy_key)
    end
    if not global_limit or global_limit > global_locks then
      found_job = redis.call("RPOP", queue) -- Sidekiq queue
      if found_job then
        redis.call("RPUSH", busy_key, capsule_uuid) -- Increment busy count
        return {queue, found_job}
      end
    end
  end
end

return nil  -- No job was found
