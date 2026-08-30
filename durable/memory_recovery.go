package durable

import (
	"context"
	"errors"
	"sort"
	"time"
)

func (s *MemoryStore) ListSteps(_ context.Context, runID string) ([]StepRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	steps := make([]StepRecord, 0)
	for _, record := range s.steps {
		if record.RunID != runID {
			continue
		}
		record.Output = cloneJSON(record.Output)
		steps = append(steps, record)
	}
	sort.SliceStable(steps, func(i, j int) bool {
		if steps[i].UpdatedAt.Equal(steps[j].UpdatedAt) {
			return steps[i].StepKey < steps[j].StepKey
		}
		return steps[i].UpdatedAt.Before(steps[j].UpdatedAt)
	})
	return steps, nil
}

func (s *MemoryStore) CreateRecoveryRun(_ context.Context, record RunRecord) error {
	if record.RunID == "" {
		return errors.New("durable: recovery run ID is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.runs[record.RunID]; exists {
		return errors.New("durable: recovery target run already exists")
	}
	now := time.Now().UTC()
	if record.Status == "" {
		record.Status = RunPending
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	if record.UpdatedAt.IsZero() {
		record.UpdatedAt = now
	}
	s.runs[record.RunID] = cloneRun(record)
	return nil
}

func (s *MemoryStore) AppendEvent(_ context.Context, event RunEvent) (RunEvent, error) {
	if event.RunID == "" || event.Type == "" {
		return RunEvent{}, errors.New("durable: event run ID and type are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	event.Sequence = int64(len(s.events[event.RunID]) + 1)
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	event.Payload = cloneJSON(event.Payload)
	s.events[event.RunID] = append(s.events[event.RunID], event)
	return event, nil
}

func (s *MemoryStore) ListEvents(_ context.Context, runID string) ([]RunEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := s.events[runID]
	result := make([]RunEvent, len(items))
	for i, event := range items {
		event.Payload = cloneJSON(event.Payload)
		result[i] = event
	}
	return result, nil
}

func (s *MemoryStore) PutDeadLetter(_ context.Context, record DeadLetterRecord) error {
	if record.RunID == "" || record.Reason == "" {
		return errors.New("durable: dead-letter run ID and reason are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	record.Payload = cloneJSON(record.Payload)
	s.deadLetters[record.RunID] = record
	return nil
}

func (s *MemoryStore) ListDeadLetters(_ context.Context, limit int) ([]DeadLetterRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]DeadLetterRecord, 0, len(s.deadLetters))
	for _, record := range s.deadLetters {
		record.Payload = cloneJSON(record.Payload)
		items = append(items, record)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}
