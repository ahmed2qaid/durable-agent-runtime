package durable

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func seedRecoveryRun(t *testing.T, store *MemoryStore, runID string) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateRecoveryRun(ctx, RunRecord{RunID: runID, Status: RunPending}); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().Add(-time.Minute)
	for i, key := range []string{"search", "approve", "publish"} {
		if err := store.PutStep(ctx, StepRecord{
			RunID: runID, StepKey: key, Status: StepCompleted,
			Output: json.RawMessage(`{"ok":true}`), Attempts: 1,
			UpdatedAt: base.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecoveryReplayCreatesFreshRunWithoutCopiedSteps(t *testing.T) {
	store := NewMemoryStore()
	seedRecoveryRun(t, store, "source")
	manager := NewRecoveryManager(store)
	if err := manager.Replay(context.Background(), "source", "replay-1"); err != nil {
		t.Fatal(err)
	}
	steps, err := store.ListSteps(context.Background(), "replay-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 0 {
		t.Fatalf("expected fresh replay with no copied checkpoints, got %d", len(steps))
	}
	events, _ := store.ListEvents(context.Background(), "replay-1")
	if len(events) != 1 || events[0].Type != EventReplayCreated {
		t.Fatalf("unexpected replay events: %+v", events)
	}
}

func TestRecoveryForkCopiesOnlyThroughCheckpoint(t *testing.T) {
	store := NewMemoryStore()
	seedRecoveryRun(t, store, "source")
	manager := NewRecoveryManager(store)
	if err := manager.Fork(context.Background(), "source", "fork-1", "approve"); err != nil {
		t.Fatal(err)
	}
	steps, err := store.ListSteps(context.Background(), "fork-1")
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(steps))
	for _, step := range steps {
		keys = append(keys, step.StepKey)
	}
	if !reflect.DeepEqual(keys, []string{"approve", "search"}) && !reflect.DeepEqual(keys, []string{"search", "approve"}) {
		t.Fatalf("unexpected forked steps: %v", keys)
	}
	if _, found, _ := store.GetStep(context.Background(), "fork-1", "publish"); found {
		t.Fatal("publish checkpoint must not be copied past fork boundary")
	}
}

func TestInvalidForkDoesNotCreateTargetRun(t *testing.T) {
	store := NewMemoryStore()
	seedRecoveryRun(t, store, "source")
	err := NewRecoveryManager(store).Fork(context.Background(), "source", "bad-fork", "missing")
	if err == nil {
		t.Fatal("expected missing checkpoint error")
	}
	if _, found, _ := store.GetRun(context.Background(), "bad-fork"); found {
		t.Fatal("invalid fork must not create a partial target run")
	}
}

func TestDeadLetterAndCompensationHistory(t *testing.T) {
	store := NewMemoryStore()
	seedRecoveryRun(t, store, "source")
	manager := NewRecoveryManager(store)
	if err := manager.DeadLetter(context.Background(), "source", "exhausted retries", json.RawMessage(`{"attempts":5}`)); err != nil {
		t.Fatal(err)
	}
	letters, err := store.ListDeadLetters(context.Background(), 10)
	if err != nil || len(letters) != 1 || letters[0].RunID != "source" {
		t.Fatalf("unexpected dead letters: %+v err=%v", letters, err)
	}

	order := []string{}
	results, err := manager.Compensate(context.Background(), "source", []Compensation{
		{Name: "reserve", Do: func(context.Context) error { order = append(order, "reserve"); return nil }},
		{Name: "charge", Do: func(context.Context) error { order = append(order, "charge"); return errors.New("refund failed") }},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"charge", "reserve"}) {
		t.Fatalf("compensations must run in reverse order: %v", order)
	}
	if len(results) != 2 || results[0].Error == "" {
		t.Fatalf("expected compensation failure to be captured: %+v", results)
	}
	events, _ := store.ListEvents(context.Background(), "source")
	if len(events) < 3 {
		t.Fatalf("expected dead-letter and compensation events, got %+v", events)
	}
}
