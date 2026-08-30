package durable

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type RunStatus string

const (
	RunPending   RunStatus = "pending"
	RunRunning   RunStatus = "running"
	RunCompleted RunStatus = "completed"
	RunFailed    RunStatus = "failed"
	RunCancelled RunStatus = "cancelled"
)

var (
	ErrLeaseHeld    = errors.New("durable: run lease is held by another worker")
	ErrLeaseLost    = errors.New("durable: run lease was lost")
	ErrRunCancelled = errors.New("durable: run cancellation requested")
)

type RunRecord struct {
	RunID             string
	Status            RunStatus
	Metadata          json.RawMessage
	LeaseOwner        string
	LeaseExpiresAt    time.Time
	CancelRequested   bool
	LastError         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	StartedAt         time.Time
	CompletedAt       time.Time
}

type RunStore interface {
	AcquireLease(ctx context.Context, runID, workerID string, ttl time.Duration) (RunRecord, error)
	Heartbeat(ctx context.Context, runID, workerID string, ttl time.Duration) (RunRecord, error)
	FinishRun(ctx context.Context, runID, workerID string, status RunStatus, lastError string) error
	RequestCancel(ctx context.Context, runID string) error
	GetRun(ctx context.Context, runID string) (RunRecord, bool, error)
}
