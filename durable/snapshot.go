package durable

import (
	"context"
	"encoding/json"
	"errors"
)

const SnapshotFormat = "durable-agent-run/v1"

type RunSnapshot struct {
	Format      string             `json:"format"`
	Run         RunRecord          `json:"run"`
	Steps       []StepRecord       `json:"steps"`
	Events      []RunEvent         `json:"events"`
	DeadLetters []DeadLetterRecord `json:"dead_letters,omitempty"`
}

func (m *RecoveryManager) Snapshot(ctx context.Context, runID string) (RunSnapshot, error) {
	run, found, err := m.Store.GetRun(ctx, runID)
	if err != nil {
		return RunSnapshot{}, err
	}
	if !found {
		return RunSnapshot{}, errors.New("durable: run not found")
	}
	steps, err := m.Store.ListSteps(ctx, runID)
	if err != nil {
		return RunSnapshot{}, err
	}
	events, err := m.Store.ListEvents(ctx, runID)
	if err != nil {
		return RunSnapshot{}, err
	}
	letters, err := m.Store.ListDeadLetters(ctx, 1000)
	if err != nil {
		return RunSnapshot{}, err
	}
	filtered := make([]DeadLetterRecord, 0, 1)
	for _, record := range letters {
		if record.RunID == runID {
			filtered = append(filtered, record)
		}
	}
	return RunSnapshot{
		Format: SnapshotFormat,
		Run: run,
		Steps: steps,
		Events: events,
		DeadLetters: filtered,
	}, nil
}

func (snapshot RunSnapshot) MarshalJSONIndent() ([]byte, error) {
	if snapshot.Format == "" {
		snapshot.Format = SnapshotFormat
	}
	return json.MarshalIndent(snapshot, "", "  ")
}
