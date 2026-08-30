package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestCompletedStepIsReplayedWithoutReexecution(t *testing.T) {
	engine := NewEngine(NewMemoryStore())
	ctx := context.Background()
	calls := 0

	fn := func(context.Context) (json.RawMessage, error) {
		calls++
		return json.RawMessage(`{"value":42}`), nil
	}

	first, err := engine.Step(ctx, "run-1", "search", fn)
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.Step(ctx, "run-1", "search", fn)
	if err != nil {
		t.Fatal(err)
	}

	if calls != 1 {
		t.Fatalf("expected one execution, got %d", calls)
	}
	if string(first) != string(second) {
		t.Fatalf("checkpoint output mismatch: %s != %s", first, second)
	}
}

func TestFailedStepCanRetry(t *testing.T) {
	store := NewMemoryStore()
	engine := NewEngine(store)
	ctx := context.Background()
	calls := 0

	fn := func(context.Context) (json.RawMessage, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("temporary failure")
		}
		return json.RawMessage(`{"ok":true}`), nil
	}

	if _, err := engine.Step(ctx, "run-2", "tool", fn); err == nil {
		t.Fatal("expected first attempt to fail")
	}
	if _, err := engine.Step(ctx, "run-2", "tool", fn); err != nil {
		t.Fatalf("expected retry to succeed: %v", err)
	}

	record, ok, err := store.GetStep(ctx, "run-2", "tool")
	if err != nil || !ok {
		t.Fatalf("expected stored step: ok=%v err=%v", ok, err)
	}
	if record.Attempts != 2 || record.Status != StepCompleted {
		t.Fatalf("unexpected record: %+v", record)
	}
}
