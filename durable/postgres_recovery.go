package durable

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

func (s *PostgresStore) ListSteps(ctx context.Context, runID string) ([]StepRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT run_id, step_key, status, COALESCE(output::text, ''), COALESCE(error, ''), attempts, updated_at
		FROM durable_steps WHERE run_id = $1 ORDER BY updated_at, step_key`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []StepRecord
	for rows.Next() {
		var record StepRecord
		var status string
		var output []byte
		if err := rows.Scan(&record.RunID, &record.StepKey, &status, &output, &record.Error, &record.Attempts, &record.UpdatedAt); err != nil {
			return nil, err
		}
		record.Status = StepStatus(status)
		if len(output) > 0 {
			record.Output = json.RawMessage(output)
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func (s *PostgresStore) CreateRecoveryRun(ctx context.Context, record RunRecord) error {
	if record.RunID == "" {
		return errors.New("durable: recovery run ID is required")
	}
	if record.Status == "" {
		record.Status = RunPending
	}
	metadata := string(record.Metadata)
	if metadata == "" {
		metadata = "{}"
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO durable_runs(id, status, metadata, created_at, updated_at)
		VALUES ($1, $2, $3::jsonb, COALESCE($4, now()), now())
		ON CONFLICT (id) DO NOTHING`, record.RunID, string(record.Status), metadata, nullableTime(record.CreatedAt))
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.New("durable: recovery target run already exists")
	}
	return nil
}

func (s *PostgresStore) AppendEvent(ctx context.Context, event RunEvent) (RunEvent, error) {
	if event.RunID == "" || event.Type == "" {
		return RunEvent{}, errors.New("durable: event run ID and type are required")
	}
	var payload any
	if event.Payload != nil {
		payload = string(event.Payload)
	}
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO durable_events(run_id, sequence, event_type, step_key, payload)
		SELECT $1,
		       COALESCE((SELECT max(sequence) + 1 FROM durable_events WHERE run_id = $1), 1),
		       $2, NULLIF($3, ''), $4::jsonb
		RETURNING sequence, created_at`, event.RunID, string(event.Type), event.StepKey, payload,
	).Scan(&event.Sequence, &event.CreatedAt)
	if err != nil {
		return RunEvent{}, err
	}
	event.Payload = cloneJSON(event.Payload)
	return event, nil
}

func (s *PostgresStore) ListEvents(ctx context.Context, runID string) ([]RunEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT sequence, run_id, event_type, COALESCE(step_key, ''), COALESCE(payload::text, ''), created_at
		FROM durable_events WHERE run_id = $1 ORDER BY sequence`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []RunEvent
	for rows.Next() {
		var event RunEvent
		var eventType string
		var payload []byte
		if err := rows.Scan(&event.Sequence, &event.RunID, &eventType, &event.StepKey, &payload, &event.CreatedAt); err != nil {
			return nil, err
		}
		event.Type = EventType(eventType)
		if len(payload) > 0 {
			event.Payload = json.RawMessage(payload)
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func (s *PostgresStore) PutDeadLetter(ctx context.Context, record DeadLetterRecord) error {
	if record.RunID == "" || record.Reason == "" {
		return errors.New("durable: dead-letter run ID and reason are required")
	}
	var payload any
	if record.Payload != nil {
		payload = string(record.Payload)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO durable_dead_letters(run_id, reason, payload, created_at)
		VALUES ($1, $2, $3::jsonb, COALESCE($4, now()))
		ON CONFLICT (run_id) DO UPDATE SET reason = EXCLUDED.reason, payload = EXCLUDED.payload, created_at = EXCLUDED.created_at`,
		record.RunID, record.Reason, payload, nullableTime(record.CreatedAt),
	)
	return err
}

func (s *PostgresStore) ListDeadLetters(ctx context.Context, limit int) ([]DeadLetterRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT run_id, reason, COALESCE(payload::text, ''), created_at
		FROM durable_dead_letters ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []DeadLetterRecord
	for rows.Next() {
		var record DeadLetterRecord
		var payload []byte
		if err := rows.Scan(&record.RunID, &record.Reason, &payload, &record.CreatedAt); err != nil {
			return nil, err
		}
		if len(payload) > 0 {
			record.Payload = json.RawMessage(payload)
		}
		result = append(result, record)
	}
	return result, rows.Err()
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

var _ RecoveryStore = (*PostgresStore)(nil)
var _ RecoveryStore = (*MemoryStore)(nil)
