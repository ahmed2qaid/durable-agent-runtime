package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var ErrApprovalPending = errors.New("durable: approval pending")
var ErrBudgetExceeded = errors.New("durable: budget exceeded")

type Usage struct {
	InputTokens  int     `json:"input_tokens"`
	OutputTokens int     `json:"output_tokens"`
	TotalTokens  int     `json:"total_tokens"`
	CostUSD      float64 `json:"cost_usd"`
}

type Budget struct {
	MaxTokens  int     `json:"max_tokens"`
	MaxCostUSD float64 `json:"max_cost_usd"`
}

type LLMRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type LLMResult struct {
	Model string `json:"model"`
	Text  string `json:"text"`
	Usage Usage  `json:"usage"`
}

type LLMInvoker func(context.Context, LLMRequest) (LLMResult, error)

type ToolRequest struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type ToolResult struct {
	Name   string          `json:"name"`
	Output json.RawMessage `json:"output"`
}

type ToolInvoker func(context.Context, ToolRequest) (ToolResult, error)

func (e *Engine) LLM(ctx context.Context, runID, stepKey string, request LLMRequest, invoke LLMInvoker) (LLMResult, error) {
	if invoke == nil {
		return LLMResult{}, errors.New("durable: LLM invoker must not be nil")
	}
	payload, err := e.Step(ctx, runID, stepKey, func(stepCtx context.Context) (json.RawMessage, error) {
		result, callErr := invoke(stepCtx, request)
		if callErr != nil {
			return nil, callErr
		}
		if result.Model == "" {
			result.Model = request.Model
		}
		encoded, marshalErr := json.Marshal(result)
		return encoded, marshalErr
	})
	if err != nil {
		return LLMResult{}, err
	}
	var result LLMResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return LLMResult{}, fmt.Errorf("durable: decode LLM checkpoint: %w", err)
	}
	return result, nil
}

func (e *Engine) LLMWithFallback(ctx context.Context, runID, stepKey, prompt string, models []string, invoke LLMInvoker) (LLMResult, error) {
	if len(models) == 0 {
		return LLMResult{}, errors.New("durable: at least one fallback model is required")
	}
	var lastErr error
	for _, model := range models {
		result, err := e.LLM(ctx, runID, stepKey, LLMRequest{Model: model, Prompt: prompt}, invoke)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return LLMResult{}, ctx.Err()
		}
	}
	return LLMResult{}, fmt.Errorf("durable: all fallback models failed: %w", lastErr)
}

func (e *Engine) Tool(ctx context.Context, runID, stepKey string, request ToolRequest, invoke ToolInvoker) (ToolResult, error) {
	if invoke == nil {
		return ToolResult{}, errors.New("durable: tool invoker must not be nil")
	}
	payload, err := e.Step(ctx, runID, stepKey, func(stepCtx context.Context) (json.RawMessage, error) {
		result, callErr := invoke(stepCtx, request)
		if callErr != nil {
			return nil, callErr
		}
		if result.Name == "" {
			result.Name = request.Name
		}
		encoded, marshalErr := json.Marshal(result)
		return encoded, marshalErr
	})
	if err != nil {
		return ToolResult{}, err
	}
	var result ToolResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return ToolResult{}, fmt.Errorf("durable: decode tool checkpoint: %w", err)
	}
	return result, nil
}

func (e *Engine) ResolveApproval(ctx context.Context, runID, stepKey string, approved bool, actor string) error {
	payload, err := json.Marshal(map[string]any{
		"approved": approved,
		"actor":    actor,
		"at":       time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	return e.store.PutStep(ctx, StepRecord{
		RunID: runID, StepKey: stepKey, Status: StepCompleted, Output: payload, Attempts: 1, UpdatedAt: time.Now().UTC(),
	})
}

func (e *Engine) Approval(ctx context.Context, runID, stepKey string) (bool, error) {
	record, found, err := e.store.GetStep(ctx, runID, stepKey)
	if err != nil {
		return false, err
	}
	if !found || record.Status != StepCompleted {
		return false, ErrApprovalPending
	}
	var payload struct {
		Approved bool `json:"approved"`
	}
	if err := json.Unmarshal(record.Output, &payload); err != nil {
		return false, fmt.Errorf("durable: decode approval checkpoint: %w", err)
	}
	return payload.Approved, nil
}

func (e *Engine) SleepUntil(ctx context.Context, runID, stepKey string, wakeAt time.Time) error {
	_, err := e.Step(ctx, runID, stepKey, func(stepCtx context.Context) (json.RawMessage, error) {
		remaining := time.Until(wakeAt)
		if remaining > 0 {
			timer := time.NewTimer(remaining)
			defer timer.Stop()
			select {
			case <-stepCtx.Done():
				return nil, stepCtx.Err()
			case <-timer.C:
			}
		}
		return json.Marshal(map[string]any{"wake_at": wakeAt.UTC()})
	})
	return err
}

func (e *Engine) CheckBudget(ctx context.Context, runID, stepKey string, usage Usage, budget Budget) error {
	_, err := e.Step(ctx, runID, stepKey, func(context.Context) (json.RawMessage, error) {
		if budget.MaxTokens > 0 && usage.TotalTokens > budget.MaxTokens {
			return nil, fmt.Errorf("%w: tokens %d > %d", ErrBudgetExceeded, usage.TotalTokens, budget.MaxTokens)
		}
		if budget.MaxCostUSD > 0 && usage.CostUSD > budget.MaxCostUSD {
			return nil, fmt.Errorf("%w: cost %.6f > %.6f", ErrBudgetExceeded, usage.CostUSD, budget.MaxCostUSD)
		}
		return json.Marshal(map[string]any{"usage": usage, "budget": budget})
	})
	return err
}
