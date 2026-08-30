# Execution Roadmap

## v0.1 — Durable step core

- [x] engine and store contracts
- [x] checkpoint completed steps
- [x] persist failed attempts
- [x] deterministic memory store
- [x] PostgreSQL schema
- [x] tests and CI

Exit criteria: a completed step can be retried after process loss without re-running its side effect.

## v0.2 — PostgreSQL driver

- transaction-safe Postgres store
- worker leases and heartbeats
- run lifecycle state
- concurrency protection
- timeout and cancellation
- retry/backoff policies

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

- multi-worker execution
- Postgres HA guidance
- deterministic recovery test suite
- OpenTelemetry integration
- SDKs/examples for agent frameworks
- benchmark and failure-injection suite
