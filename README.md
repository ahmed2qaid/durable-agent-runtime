# Durable Agent Runtime

A small, Postgres-first durable execution runtime designed for AI agents.

The core promise is simple: **completed steps are not repeated after a crash or retry**.

```text
Agent code
   ↓
Durable Engine
   ↓
Step checkpoints
   ↓
PostgreSQL
```

## v0.1

- durable step API
- pluggable persistence `Store` contract
- in-memory store for deterministic tests
- PostgreSQL schema for runs and steps
- retry-safe completed-step replay
- failure state persistence
- CI and unit tests

## Example

```go
engine := durable.NewEngine(store)

output, err := engine.Step(ctx, "run-123", "search", func(ctx context.Context) (json.RawMessage, error) {
    return json.RawMessage(`{"result":"done"}`), nil
})
```

Calling the same completed step again returns the checkpointed output without executing the function again.

## Why agent-specific?

Future versions add semantics that generic job queues do not model directly: LLM call persistence, tool-call idempotency, human approval waits, model fallback, agent checkpoints, trajectory history, budget state, and run forking.

See [ROADMAP.md](ROADMAP.md).

## Status

v0.1 foundation.

## License

MIT.