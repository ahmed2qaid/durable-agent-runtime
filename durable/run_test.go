package durable

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLeasePreventsConcurrentWorker(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if _, err := store.AcquireLease(ctx, "run-1", "worker-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireLease(ctx, "run-1", "worker-b", time.Minute); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("expected ErrLeaseHeld, got %v", err)
	}
}

func TestHeartbeatRejectsWrongOwner(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if _, err := store.AcquireLease(ctx, "run-1", "worker-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Heartbeat(ctx, "run-1", "worker-b", time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost, got %v", err)
	}
}

func TestCancellationBlocksNewLease(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.RequestCancel(ctx, "run-cancel"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireLease(ctx, "run-cancel", "worker-a", time.Minute); !errors.Is(err, ErrRunCancelled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
}

func TestStepWithRetryPersistsAttempts(t *testing.T) {
	store := NewMemoryStore()
	engine := NewEngine(store)
	calls := 0
	output, err := engine.StepWithRetry(context.Background(), "run", "unstable", RetryPolicy{MaxAttempts: 3}, func(context.Context) ([]byte, error) {
		calls++
		if calls < 3 {
			return nil, errors.New("temporary")
		}
		return []byte(`{"ok":true}`), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != `{"ok":true}` || calls != 3 {
		t.Fatalf("unexpected output=%s calls=%d", output, calls)
	}
	record, found, err := store.GetStep(context.Background(), "run", "unstable")
	if err != nil || !found || record.Attempts != 3 || record.Status != StepCompleted {
		t.Fatalf("unexpected record: %+v found=%v err=%v", record, found, err)
	}
}

func TestWorkerCompletesRun(t *testing.T) {
	store := NewMemoryStore()
	worker := Worker{Store: store, ID: "worker-1", LeaseTTL: time.Second, HeartbeatInterval: 100 * time.Millisecond}
	if err := worker.Execute(context.Background(), "run-worker", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	run, found, err := store.GetRun(context.Background(), "run-worker")
	if err != nil || !found || run.Status != RunCompleted || run.LeaseOwner != "" {
		t.Fatalf("unexpected run: %+v found=%v err=%v", run, found, err)
	}
}
