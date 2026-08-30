package durable

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

type MemoryStore struct {
	mu          sync.RWMutex
	steps       map[string]StepRecord
	runs        map[string]RunRecord
	events      map[string][]RunEvent
	deadLetters map[string]DeadLetterRecord
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		steps:       make(map[string]StepRecord),
		runs:        make(map[string]RunRecord),
		events:      make(map[string][]RunEvent),
		deadLetters: make(map[string]DeadLetterRecord),
	}
}

func (s *MemoryStore) GetStep(_ context.Context, runID, stepKey string) (StepRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.steps[runID+"\x00"+stepKey]
	if !ok {
		return StepRecord{}, false, nil
	}
	record.Output = cloneJSON(record.Output)
	return record, true, nil
}

func (s *MemoryStore) PutStep(_ context.Context, record StepRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record.Output = cloneJSON(record.Output)
	s.steps[record.RunID+"\x00"+record.StepKey] = record
	return nil
}

func (s *MemoryStore) AcquireLease(_ context.Context, runID, workerID string, ttl time.Duration) (RunRecord, error) {
	if runID == "" || workerID == "" || ttl <= 0 {
		return RunRecord{}, errors.New("durable: runID, workerID, and positive ttl are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	run, ok := s.runs[runID]
	if !ok {
		run = RunRecord{RunID: runID, Status: RunPending, Metadata: json.RawMessage(`{}`), CreatedAt: now}
	}
	if run.CancelRequested {
		return RunRecord{}, ErrRunCancelled
	}
	if run.Status == RunCompleted || run.Status == RunFailed || run.Status == RunCancelled {
		return RunRecord{}, errors.New("durable: run is already terminal")
	}
	if run.LeaseOwner != "" && run.LeaseOwner != workerID && run.LeaseExpiresAt.After(now) {
		return RunRecord{}, ErrLeaseHeld
	}
	run.Status = RunRunning
	run.LeaseOwner = workerID
	run.LeaseExpiresAt = now.Add(ttl)
	if run.StartedAt.IsZero() {
		run.StartedAt = now
	}
	run.UpdatedAt = now
	s.runs[runID] = run
	return cloneRun(run), nil
}

func (s *MemoryStore) Heartbeat(_ context.Context, runID, workerID string, ttl time.Duration) (RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	run, ok := s.runs[runID]
	if !ok || run.LeaseOwner != workerID || !run.LeaseExpiresAt.After(now) {
		return RunRecord{}, ErrLeaseLost
	}
	if run.CancelRequested {
		return RunRecord{}, ErrRunCancelled
	}
	run.LeaseExpiresAt = now.Add(ttl)
	run.UpdatedAt = now
	s.runs[runID] = run
	return cloneRun(run), nil
}

func (s *MemoryStore) FinishRun(_ context.Context, runID, workerID string, status RunStatus, lastError string) error {
	if status != RunCompleted && status != RunFailed && status != RunCancelled {
		return errors.New("durable: finish status must be terminal")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok || run.LeaseOwner != workerID {
		return ErrLeaseLost
	}
	now := time.Now().UTC()
	run.Status = status
	run.LastError = lastError
	run.LeaseOwner = ""
	run.LeaseExpiresAt = time.Time{}
	run.CompletedAt = now
	run.UpdatedAt = now
	s.runs[runID] = run
	return nil
}

func (s *MemoryStore) RequestCancel(_ context.Context, runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	run, ok := s.runs[runID]
	if !ok {
		run = RunRecord{RunID: runID, Status: RunPending, Metadata: json.RawMessage(`{}`), CreatedAt: now}
	}
	run.CancelRequested = true
	run.UpdatedAt = now
	s.runs[runID] = run
	return nil
}

func (s *MemoryStore) GetRun(_ context.Context, runID string) (RunRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	run, ok := s.runs[runID]
	if !ok {
		return RunRecord{}, false, nil
	}
	return cloneRun(run), true, nil
}

func cloneRun(run RunRecord) RunRecord {
	run.Metadata = cloneJSON(run.Metadata)
	return run
}
