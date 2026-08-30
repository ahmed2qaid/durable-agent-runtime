package durable

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// PostgresStore implements checkpoint and run-lease persistence using PostgreSQL SQL semantics.
// The caller owns the *sql.DB and may register pgx/libpq or another PostgreSQL database/sql driver.
type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	if db == nil {
		panic("durable: postgres db must not be nil")
	}
	return &PostgresStore{db: db}
}

func (s *PostgresStore) GetStep(ctx context.Context, runID, stepKey string) (StepRecord, bool, error) {
	var record StepRecord
	var status string
	var output []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT run_id, step_key, status, COALESCE(output::text, ''), COALESCE(error, ''), attempts, updated_at
		FROM durable_steps WHERE run_id = $1 AND step_key = $2`, runID, stepKey,
	).Scan(&record.RunID, &record.StepKey, &status, &output, &record.Error, &record.Attempts, &record.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return StepRecord{}, false, nil
	}
	if err != nil {
		return StepRecord{}, false, err
	}
	record.Status = StepStatus(status)
	if len(output) > 0 {
		record.Output = json.RawMessage(output)
	}
	return record, true, nil
}

func (s *PostgresStore) PutStep(ctx context.Context, record StepRecord) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx, `
		INSERT INTO durable_runs(id, status) VALUES ($1, 'pending')
		ON CONFLICT (id) DO NOTHING`, record.RunID); err != nil {
		return err
	}

	var output any
	if record.Output != nil {
		output = string(record.Output)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO durable_steps(run_id, step_key, status, output, error, attempts, updated_at)
		VALUES ($1, $2, $3, $4::jsonb, NULLIF($5, ''), $6, $7)
		ON CONFLICT (run_id, step_key) DO UPDATE SET
			status = EXCLUDED.status,
			output = EXCLUDED.output,
			error = EXCLUDED.error,
			attempts = EXCLUDED.attempts,
			updated_at = EXCLUDED.updated_at`,
		record.RunID, record.StepKey, string(record.Status), output, record.Error, record.Attempts, record.UpdatedAt,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PostgresStore) AcquireLease(ctx context.Context, runID, workerID string, ttl time.Duration) (RunRecord, error) {
	if runID == "" || workerID == "" || ttl <= 0 {
		return RunRecord{}, errors.New("durable: runID, workerID, and positive ttl are required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return RunRecord{}, err
	}
	defer tx.Rollback()

	if _, err = tx.ExecContext(ctx, `
		INSERT INTO durable_runs(id, status) VALUES ($1, 'pending')
		ON CONFLICT (id) DO NOTHING`, runID); err != nil {
		return RunRecord{}, err
	}

	row := tx.QueryRowContext(ctx, `
		UPDATE durable_runs
		SET status = 'running', lease_owner = $2,
			lease_expires_at = now() + ($3 * interval '1 second'),
			started_at = COALESCE(started_at, now()), updated_at = now()
		WHERE id = $1
		  AND cancel_requested = false
		  AND status IN ('pending', 'running')
		  AND (lease_owner = $2 OR lease_expires_at IS NULL OR lease_expires_at <= now())
		RETURNING id, status, metadata::text, COALESCE(lease_owner, ''), lease_expires_at,
			cancel_requested, COALESCE(last_error, ''), created_at, updated_at, started_at, completed_at`,
		runID, workerID, ttl.Seconds(),
	)
	run, err := scanRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		var cancel bool
		checkErr := tx.QueryRowContext(ctx, `SELECT cancel_requested FROM durable_runs WHERE id = $1`, runID).Scan(&cancel)
		if checkErr == nil && cancel {
			return RunRecord{}, ErrRunCancelled
		}
		return RunRecord{}, ErrLeaseHeld
	}
	if err != nil {
		return RunRecord{}, err
	}
	if err = tx.Commit(); err != nil {
		return RunRecord{}, err
	}
	return run, nil
}

func (s *PostgresStore) Heartbeat(ctx context.Context, runID, workerID string, ttl time.Duration) (RunRecord, error) {
	row := s.db.QueryRowContext(ctx, `
		UPDATE durable_runs
		SET lease_expires_at = now() + ($3 * interval '1 second'), updated_at = now()
		WHERE id = $1 AND status = 'running' AND lease_owner = $2
		  AND lease_expires_at > now() AND cancel_requested = false
		RETURNING id, status, metadata::text, COALESCE(lease_owner, ''), lease_expires_at,
			cancel_requested, COALESCE(last_error, ''), created_at, updated_at, started_at, completed_at`,
		runID, workerID, ttl.Seconds(),
	)
	run, err := scanRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		var cancel bool
		if checkErr := s.db.QueryRowContext(ctx, `SELECT cancel_requested FROM durable_runs WHERE id = $1`, runID).Scan(&cancel); checkErr == nil && cancel {
			return RunRecord{}, ErrRunCancelled
		}
		return RunRecord{}, ErrLeaseLost
	}
	return run, err
}

func (s *PostgresStore) FinishRun(ctx context.Context, runID, workerID string, status RunStatus, lastError string) error {
	if status != RunCompleted && status != RunFailed && status != RunCancelled {
		return errors.New("durable: finish status must be terminal")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE durable_runs SET status = $3, last_error = NULLIF($4, ''),
			lease_owner = NULL, lease_expires_at = NULL, completed_at = now(), updated_at = now()
		WHERE id = $1 AND lease_owner = $2`, runID, workerID, string(status), lastError)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *PostgresStore) RequestCancel(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO durable_runs(id, status, cancel_requested) VALUES ($1, 'pending', true)
		ON CONFLICT (id) DO UPDATE SET cancel_requested = true, updated_at = now()`, runID)
	return err
}

func (s *PostgresStore) GetRun(ctx context.Context, runID string) (RunRecord, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, status, metadata::text, COALESCE(lease_owner, ''), lease_expires_at,
			cancel_requested, COALESCE(last_error, ''), created_at, updated_at, started_at, completed_at
		FROM durable_runs WHERE id = $1`, runID)
	run, err := scanRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		return RunRecord{}, false, nil
	}
	if err != nil {
		return RunRecord{}, false, err
	}
	return run, true, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRun(row rowScanner) (RunRecord, error) {
	var run RunRecord
	var status string
	var metadata []byte
	var leaseExpires, startedAt, completedAt sql.NullTime
	if err := row.Scan(
		&run.RunID, &status, &metadata, &run.LeaseOwner, &leaseExpires,
		&run.CancelRequested, &run.LastError, &run.CreatedAt, &run.UpdatedAt, &startedAt, &completedAt,
	); err != nil {
		return RunRecord{}, err
	}
	run.Status = RunStatus(status)
	run.Metadata = json.RawMessage(metadata)
	if leaseExpires.Valid {
		run.LeaseExpiresAt = leaseExpires.Time
	}
	if startedAt.Valid {
		run.StartedAt = startedAt.Time
	}
	if completedAt.Valid {
		run.CompletedAt = completedAt.Time
	}
	return run, nil
}

func (s *PostgresStore) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("durable: postgres ping: %w", err)
	}
	return nil
}
