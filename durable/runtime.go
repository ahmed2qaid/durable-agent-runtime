package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepCompleted StepStatus = "completed"
	StepFailed    StepStatus = "failed"
)

type StepRecord struct {
	RunID     string
	StepKey   string
	Status    StepStatus
	Output    json.RawMessage
	Error     string
	Attempts  int
	UpdatedAt time.Time
}

type Store interface {
	GetStep(ctx context.Context, runID, stepKey string) (StepRecord, bool, error)
	PutStep(ctx context.Context, record StepRecord) error
}

type Engine struct {
	store Store
}

func NewEngine(store Store) *Engine {
	if store == nil {
		panic("durable: store must not be nil")
	}
	return &Engine{store: store}
}

type StepFunc func(context.Context) (json.RawMessage, error)

func (e *Engine) Step(ctx context.Context, runID, stepKey string, fn StepFunc) (json.RawMessage, error) {
	if runID == "" {
		return nil, errors.New("durable: runID must not be empty")
	}
	if stepKey == "" {
		return nil, errors.New("durable: stepKey must not be empty")
	}
	if fn == nil {
		return nil, errors.New("durable: step function must not be nil")
	}

	previous, found, err := e.store.GetStep(ctx, runID, stepKey)
	if err != nil {
		return nil, fmt.Errorf("durable: load checkpoint: %w", err)
	}
	if found && previous.Status == StepCompleted {
		return cloneJSON(previous.Output), nil
	}

	attempt := 1
	if found {
		attempt = previous.Attempts + 1
	}

	output, runErr := fn(ctx)
	record := StepRecord{
		RunID:     runID,
		StepKey:   stepKey,
		Attempts:  attempt,
		UpdatedAt: time.Now().UTC(),
	}

	if runErr != nil {
		record.Status = StepFailed
		record.Error = runErr.Error()
		if err := e.store.PutStep(ctx, record); err != nil {
			return nil, fmt.Errorf("durable: persist failed attempt: %w", err)
		}
		return nil, runErr
	}

	record.Status = StepCompleted
	record.Output = cloneJSON(output)
	if err := e.store.PutStep(ctx, record); err != nil {
		return nil, fmt.Errorf("durable: persist completed step: %w", err)
	}
	return cloneJSON(output), nil
}

func cloneJSON(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	copyValue := make([]byte, len(value))
	copy(copyValue, value)
	return json.RawMessage(copyValue)
}
