package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

type EventType string

const (
	EventRunCreated    EventType = "run.created"
	EventReplayCreated EventType = "run.replay_created"
	EventForkCreated   EventType = "run.fork_created"
	EventCompensation  EventType = "run.compensation"
	EventDeadLettered  EventType = "run.dead_lettered"
)

type RunEvent struct {
	Sequence  int64
	RunID     string
	Type      EventType
	StepKey   string
	Payload   json.RawMessage
	CreatedAt time.Time
}

type DeadLetterRecord struct {
	RunID     string
	Reason    string
	Payload   json.RawMessage
	CreatedAt time.Time
}

type RecoveryStore interface {
	Store
	RunStore
	ListSteps(ctx context.Context, runID string) ([]StepRecord, error)
	CreateRecoveryRun(ctx context.Context, record RunRecord) error
	AppendEvent(ctx context.Context, event RunEvent) (RunEvent, error)
	ListEvents(ctx context.Context, runID string) ([]RunEvent, error)
	PutDeadLetter(ctx context.Context, record DeadLetterRecord) error
	ListDeadLetters(ctx context.Context, limit int) ([]DeadLetterRecord, error)
}

type RecoveryManager struct {
	Store RecoveryStore
}

func NewRecoveryManager(store RecoveryStore) *RecoveryManager {
	if store == nil {
		panic("durable: recovery store must not be nil")
	}
	return &RecoveryManager{Store: store}
}

func (m *RecoveryManager) Replay(ctx context.Context, sourceRunID, targetRunID string) error {
	if sourceRunID == "" || targetRunID == "" || sourceRunID == targetRunID {
		return errors.New("durable: replay requires distinct source and target run IDs")
	}
	if _, found, err := m.Store.GetRun(ctx, sourceRunID); err != nil {
		return err
	} else if !found {
		return fmt.Errorf("durable: source run not found: %s", sourceRunID)
	}
	metadata, _ := json.Marshal(map[string]any{"replay_of": sourceRunID})
	now := time.Now().UTC()
	if err := m.Store.CreateRecoveryRun(ctx, RunRecord{
		RunID: targetRunID, Status: RunPending, Metadata: metadata, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return err
	}
	payload, _ := json.Marshal(map[string]any{"source_run_id": sourceRunID})
	_, err := m.Store.AppendEvent(ctx, RunEvent{RunID: targetRunID, Type: EventReplayCreated, Payload: payload})
	return err
}

func (m *RecoveryManager) Fork(ctx context.Context, sourceRunID, targetRunID, throughStep string) error {
	if sourceRunID == "" || targetRunID == "" || throughStep == "" || sourceRunID == targetRunID {
		return errors.New("durable: fork requires source, target, and checkpoint step")
	}
	steps, err := m.Store.ListSteps(ctx, sourceRunID)
	if err != nil {
		return err
	}
	if len(steps) == 0 {
		return fmt.Errorf("durable: source run has no checkpoints: %s", sourceRunID)
	}
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].UpdatedAt.Before(steps[j].UpdatedAt) })

	selected := make([]StepRecord, 0, len(steps))
	foundCheckpoint := false
	for _, step := range steps {
		if step.Status != StepCompleted {
			continue
		}
		selected = append(selected, step)
		if step.StepKey == throughStep {
			foundCheckpoint = true
			break
		}
	}
	if !foundCheckpoint {
		return fmt.Errorf("durable: completed checkpoint not found: %s", throughStep)
	}

	metadata, _ := json.Marshal(map[string]any{"fork_of": sourceRunID, "through_step": throughStep})
	now := time.Now().UTC()
	if err := m.Store.CreateRecoveryRun(ctx, RunRecord{
		RunID: targetRunID, Status: RunPending, Metadata: metadata, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		return err
	}
	for _, step := range selected {
		step.RunID = targetRunID
		step.UpdatedAt = now
		if err := m.Store.PutStep(ctx, step); err != nil {
			return err
		}
	}
	payload, _ := json.Marshal(map[string]any{
		"source_run_id": sourceRunID,
		"through_step": throughStep,
		"copied_steps": len(selected),
	})
	_, err = m.Store.AppendEvent(ctx, RunEvent{RunID: targetRunID, Type: EventForkCreated, StepKey: throughStep, Payload: payload})
	return err
}

func (m *RecoveryManager) DeadLetter(ctx context.Context, runID, reason string, payload json.RawMessage) error {
	if runID == "" || reason == "" {
		return errors.New("durable: dead-letter run ID and reason are required")
	}
	record := DeadLetterRecord{RunID: runID, Reason: reason, Payload: cloneJSON(payload), CreatedAt: time.Now().UTC()}
	if err := m.Store.PutDeadLetter(ctx, record); err != nil {
		return err
	}
	eventPayload, _ := json.Marshal(map[string]any{"reason": reason})
	_, err := m.Store.AppendEvent(ctx, RunEvent{RunID: runID, Type: EventDeadLettered, Payload: eventPayload})
	return err
}

type Compensation struct {
	Name string
	Do   func(context.Context) error
}

type CompensationResult struct {
	Name  string
	Error string
}

func (m *RecoveryManager) Compensate(ctx context.Context, runID string, actions []Compensation) ([]CompensationResult, error) {
	if runID == "" {
		return nil, errors.New("durable: run ID is required for compensation")
	}
	results := make([]CompensationResult, 0, len(actions))
	for i := len(actions) - 1; i >= 0; i-- {
		action := actions[i]
		result := CompensationResult{Name: action.Name}
		if action.Name == "" || action.Do == nil {
			result.Error = "invalid compensation action"
		} else if err := action.Do(ctx); err != nil {
			result.Error = err.Error()
		}
		results = append(results, result)
		payload, _ := json.Marshal(result)
		if _, err := m.Store.AppendEvent(ctx, RunEvent{
			RunID: runID, Type: EventCompensation, StepKey: action.Name, Payload: payload,
		}); err != nil {
			return results, err
		}
	}
	return results, nil
}
