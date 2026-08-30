package durable

import (
	"context"
	"errors"
	"time"
)

type Worker struct {
	Store             RunStore
	ID                string
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
}

func (w Worker) validate() error {
	if w.Store == nil || w.ID == "" {
		return errors.New("durable: worker store and ID are required")
	}
	if w.LeaseTTL <= 0 {
		return errors.New("durable: LeaseTTL must be positive")
	}
	if w.HeartbeatInterval <= 0 || w.HeartbeatInterval >= w.LeaseTTL {
		return errors.New("durable: HeartbeatInterval must be positive and less than LeaseTTL")
	}
	return nil
}

func (w Worker) Execute(ctx context.Context, runID string, fn func(context.Context) error) error {
	if err := w.validate(); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("durable: run function must not be nil")
	}
	if _, err := w.Store.AcquireLease(ctx, runID, w.ID, w.LeaseTTL); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	heartbeatErr := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(w.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-runCtx.Done():
				return
			case <-ticker.C:
				if _, err := w.Store.Heartbeat(runCtx, runID, w.ID, w.LeaseTTL); err != nil {
					select { case heartbeatErr <- err: default: }
					cancel()
					return
				}
			}
		}
	}()

	runErr := fn(runCtx)
	close(done)

	select {
	case leaseErr := <-heartbeatErr:
		status := RunFailed
		if errors.Is(leaseErr, ErrRunCancelled) {
			status = RunCancelled
		}
		_ = w.Store.FinishRun(context.Background(), runID, w.ID, status, leaseErr.Error())
		return leaseErr
	default:
	}

	if runErr != nil {
		status := RunFailed
		if errors.Is(runErr, context.Canceled) && ctx.Err() != nil {
			status = RunCancelled
		}
		finishErr := w.Store.FinishRun(context.Background(), runID, w.ID, status, runErr.Error())
		if finishErr != nil {
			return finishErr
		}
		return runErr
	}
	return w.Store.FinishRun(context.Background(), runID, w.ID, RunCompleted, "")
}
