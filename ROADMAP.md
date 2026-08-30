# Execution Roadmap

## v0.1 — Durable step core

- [x] engine and store contracts
- [x] checkpoint completed steps
- [x] persist failed attempts
- [x] deterministic memory store
- [x] PostgreSQL schema
- [x] tests and CI

## v0.2 — PostgreSQL driver and worker ownership

- [x] transaction-safe PostgreSQL step/run stores
- [x] worker leases with expiration and heartbeat renewal
- [x] run lifecycle states and cancellation
- [x] concurrency protection and crash takeover
- [x] retry/backoff policies and per-attempt timeouts

## v0.3 — Agent primitives

- [x] durable LLM calls with serialized checkpoints
- [x] durable side-effecting tool calls
- [x] human approval wait/resolve checkpoints
- [x] durable sleep/timers that resume from wall-clock target
- [x] ordered model fallback
- [x] cost/token budget checkpoints
- [x] tests proving completed LLM/tool side effects are not re-executed

Exit criteria: common agent operations can survive process loss and resume from durable checkpoints without paying for or repeating already-completed model/tool calls.

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
