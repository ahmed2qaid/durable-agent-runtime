package durable

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu    sync.RWMutex
	steps map[string]StepRecord
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{steps: make(map[string]StepRecord)}
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
