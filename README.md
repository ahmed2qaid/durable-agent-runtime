# Durable Agent Runtime

A Postgres-first durable execution core for AI agents: checkpoint expensive side effects, lease work to one worker, heartbeat ownership, and recover safely after crashes.

```text
Agent / Worker
      ↓
 Durable Runtime
  ├─ step checkpointing
  ├─ retry + timeout
  ├─ run lease
  ├─ heartbeat
  └─ cancellation
      ↓
 PostgreSQL
```

## v0.2

- deterministic durable steps from v0.1
- transaction-safe PostgreSQL checkpoint store
- PostgreSQL run lifecycle store
- worker leases with expiration
- heartbeat-based ownership renewal
- concurrency protection across workers
- cancellation requests
- terminal run lifecycle: completed / failed / cancelled
- exponential retry/backoff policy
- per-attempt timeouts
- memory implementation for deterministic tests
- migrations and CI

## Durable steps

```go
store := durable.NewPostgresStore(db)
engine := durable.NewEngine(store)

result, err := engine.Step(ctx, "run-42", "charge-card", func(ctx context.Context) (json.RawMessage, error) {
    return chargeOnce(ctx)
})
```

Once a step is persisted as completed, replay returns its saved output rather than repeating the external side effect.

## Retry and timeout

```go
result, err := engine.StepWithRetry(ctx, "run-42", "model-call", durable.RetryPolicy{
    MaxAttempts:       4,
    BaseDelay:         250 * time.Millisecond,
    MaxDelay:          5 * time.Second,
    TimeoutPerAttempt: 30 * time.Second,
}, callModel)
```

Each failed attempt is checkpointed before retrying.

## Worker lease

```go
worker := durable.Worker{
    Store:             store,
    ID:                "worker-eu-01",
    LeaseTTL:          30 * time.Second,
    HeartbeatInterval: 10 * time.Second,
}

err := worker.Execute(ctx, "run-42", func(ctx context.Context) error {
    // Execute the durable agent workflow here.
    return nil
})
```

A second worker cannot claim the same unexpired run. If the owner crashes and its lease expires, another worker can acquire the run and continue from persisted step checkpoints.

## PostgreSQL

Apply:

```text
migrations/001_init.sql
migrations/002_leases.sql
```

`NewPostgresStore` accepts a standard `*sql.DB`; the application chooses its PostgreSQL driver (for example pgx's `stdlib`) instead of the runtime forcing a driver dependency.

## Current boundary

v0.2 provides durable ownership and recovery primitives. Durable LLM/tool helpers, approvals, timers, model fallback, and budget checkpoints are the v0.3 layer.

See [ROADMAP.md](ROADMAP.md).

## License

MIT.
