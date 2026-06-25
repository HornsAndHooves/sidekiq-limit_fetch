# Changelog

## [5.0.0] - 2026-06-18

This release is a complete rewrite of the gem for Sidekiq 8+.

- Replace the legacy fetch implementation with a new `Sidekiq::LimitFetch` strategy built for Sidekiq 8's capsule/fetch model.
- Enforce queue concurrency limits atomically with a Redis Lua script that checks limits, pops jobs, and records busy slots in a single operation.
- Add Redis-backed global queue limits and per-process queue limits.
- Add heartbeat-based process registration and stale busy-slot reaping so queues recover after unclean Sidekiq shutdowns.
- Preserve Sidekiq's normal strict and weighted queue ordering while skipping queues whose limits are currently exhausted.
- Remove legacy APIs/features from older implementations, including queue pause/unpause helpers, blocking queue mode, dynamic queues, and `Sidekiq::Queue[...]` limit helpers.

## [4.5.0] - 2026-06-15

This project was taken over by [@HornsAndHooves](https://github.com/HornsAndHooves)

- Remove compatibility with Sidekiq < 8
- Fix issue with queue weights specified via CLI not processing jobs

## [4.3.2] - 2022-09-01

- #139 - Fix Redis deprecation warnings from [@adamzapasnik](https://github.com/adamzapasnik)

## [4.3.1] - 2022-08-23

- #137 - Fix deprecation of passing timeout as positional argument to brpop from [@cgunther](https://github.com/cgunther)

## [4.3.0] - 2022-08-16

- #135 - Some extra fixes for Sidekiq 6.5 (fixes #128, #130, #131) from [@BobbyMcWho](https://github.com/BobbyMcWho)

## [4.2.0] - 2022-06-09

- #127 - Fix for Sidekiq 6.5 internal change vias PR #128 from [@evgeniradev][https://github.com/evgeniradev]
- testing changes: stop supporting Sidekiq < 6, add tests for Sidekiq 6.5, stop testing on ruby 2.6 EOL

## [4.1.0] - 2022-03-29

- #101 - Fix stuck queues bug on Redis restart from [@907th](https://github.com/907th).

## [4.0.0] - 2022-03-26

This project was taken over by [@deanpcmad](https://github.com/deanpcmad)

- #120 - Migrate CI to GitHub Actions from [@petergoldstein](https://github.com/petergoldstein).
- #124 - Fixed redis v4.6.0 pipelines deprecation warning from [@iurev](https://github.com/iurev).
- #83  - Processing dynamic queues from [@alexey-yanchenko](https://github.com/alexey-yanchenko).
