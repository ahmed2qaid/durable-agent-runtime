# Demo Integration Contract

This repository remains the durable execution product. The end-to-end demo uses it to survive pauses, approvals, retries and process restarts without repeating completed side effects.

## Role in `ai-automation-infra-demo`

```text
Orchestrator
    ↓ durable steps
Durable Agent Runtime
    ↓ checkpoints / leases / events
PostgreSQL
```

## Demo responsibilities

- checkpoint model/tool decisions that must not be repeated after restart
- pause a high-risk action while human approval is pending
- resume from the approval checkpoint without re-running earlier completed work
- demonstrate retry/backoff on one intentionally flaky step
- expose recovery history for operator inspection
- support replay or fork of a completed/failed reference run

## Stable integration surface

The demo should consume the Go module as a dependency or run a thin demo worker that depends on it. Durable semantics must not be reimplemented in the integration repository.

PostgreSQL is shared infrastructure in the demo stack, but durable runtime tables remain logically owned by this project.

## Reference scenario

A refund flow demonstrates the value clearly:

1. classify request;
2. prepare refund parameters;
3. call the protected MCP tool;
4. receive approval-required state;
5. persist wait checkpoint;
6. simulate worker restart;
7. approve;
8. resume from checkpoint;
9. execute the side effect exactly once;
10. persist completion and event history.

## Boundary rule

Checkpointing, leases, replay/fork, dead-letter and compensation behavior belongs in `durable-agent-runtime`. The demo only supplies a realistic workflow around those primitives.
