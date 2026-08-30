package durable

import (
	"context"
	"errors"
	"math"
	"time"
)

type RetryPolicy struct {
	MaxAttempts       int
	BaseDelay         time.Duration
	MaxDelay          time.Duration
	TimeoutPerAttempt time.Duration
	Retryable         func(error) bool
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, BaseDelay: 250 * time.Millisecond, MaxDelay: 5 * time.Second}
}

func (p RetryPolicy) normalized() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 1
	}
	if p.BaseDelay < 0 {
		p.BaseDelay = 0
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = p.BaseDelay
	}
	return p
}

func (p RetryPolicy) delay(attempt int) time.Duration {
	p = p.normalized()
	if p.BaseDelay == 0 {
		return 0
	}
	delay := time.Duration(float64(p.BaseDelay) * math.Pow(2, float64(attempt-1)))
	if delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

func (e *Engine) StepWithRetry(ctx context.Context, runID, stepKey string, policy RetryPolicy, fn StepFunc) (jsonOutput []byte, err error) {
	policy = policy.normalized()
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		attemptCtx := ctx
		cancel := func() {}
		if policy.TimeoutPerAttempt > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, policy.TimeoutPerAttempt)
		}
		output, runErr := e.Step(attemptCtx, runID, stepKey, fn)
		cancel()
		if runErr == nil {
			return output, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if policy.Retryable != nil && !policy.Retryable(runErr) {
			return nil, runErr
		}
		if errors.Is(runErr, context.Canceled) && policy.TimeoutPerAttempt == 0 {
			return nil, runErr
		}
		if attempt == policy.MaxAttempts {
			return nil, runErr
		}
		delay := policy.delay(attempt)
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, errors.New("durable: retry loop exhausted")
}
