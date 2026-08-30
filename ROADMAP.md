# Execution Roadmap

## v0.1 — Durable step core

- [x] engine and store contracts
- [x] checkpoint completed steps
- [x] persist failed attempts
- [x] deterministic memory store
- [x] PostgreSQL schema
- [x] tests and CI

## v0.2 — PostgreSQL driver and worker ownership

- [x] transaction-safe PostgreSQL step store
- [x] PostgreSQL run lifecycle store
- [x] worker leases with expiration
- [x] heartbeat renewal
- [x] run lifecycle states
- [x] concurrency protection
- [x] cancellation requests
- [x] retry/backoff policies
- [x] per-attempt timeout support
- [x] deterministic in-memory lease tests

Exit criteria: only one worker owns an active run lease at a time, a crashed worker can be replaced after lease expiry, and completed durable steps are reused rather than replaying side effects.

## v0.3 — Agent primitives

- durable LLM calls
- durable tool calls
- human approval waits
- durable sleep/timers
- model fallback
- cost/token budget checkpoints

## v0.4 — Recovery and replay

- event history
- run replay
- fork from checkpoint
- dead-letter runs
- compensation hooks
- operator CLI

## v1.0

- multi-worker failure-injection suite
- Postgres HA guidance
- deterministic recovery compatibility suite
- OpenTelemetry integration
- SDKs/examples for agent frameworks
- benchmark and overhead report
