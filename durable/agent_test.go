package durable

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestLLMCheckpointPreventsDuplicateCall(t *testing.T) {
	engine := NewEngine(NewMemoryStore())
	calls := 0
	invoke := func(context.Context, LLMRequest) (LLMResult, error) {
		calls++
		return LLMResult{Text: "ok", Usage: Usage{TotalTokens: 10, CostUSD: 0.01}}, nil
	}
	request := LLMRequest{Model: "model-a", Prompt: "hello"}
	first, err := engine.LLM(context.Background(), "run", "llm", request, invoke)
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.LLM(context.Background(), "run", "llm", request, invoke)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || first.Text != second.Text || second.Model != "model-a" {
		t.Fatalf("checkpoint failed calls=%d first=%+v second=%+v", calls, first, second)
	}
}

func TestLLMFallbackPersistsSuccessfulModel(t *testing.T) {
	engine := NewEngine(NewMemoryStore())
	calls := 0
	invoke := func(_ context.Context, request LLMRequest) (LLMResult, error) {
		calls++
		if request.Model == "bad" {
			return LLMResult{}, errors.New("provider down")
		}
		return LLMResult{Model: request.Model, Text: "fallback"}, nil
	}
	result, err := engine.LLMWithFallback(context.Background(), "run", "llm", "hello", []string{"bad", "good"}, invoke)
	if err != nil {
		t.Fatal(err)
	}
	if result.Model != "good" || calls != 2 {
		t.Fatalf("unexpected result=%+v calls=%d", result, calls)
	}
	_, err = engine.LLMWithFallback(context.Background(), "run", "llm", "hello", []string{"bad", "good"}, invoke)
	if err != nil || calls != 2 {
		t.Fatalf("completed fallback was re-executed: calls=%d err=%v", calls, err)
	}
}

func TestToolCheckpointPreventsDuplicateSideEffect(t *testing.T) {
	engine := NewEngine(NewMemoryStore())
	calls := 0
	request := ToolRequest{Name: "send_email", Arguments: json.RawMessage(`{"to":"a@example.com"}`)}
	invoke := func(context.Context, ToolRequest) (ToolResult, error) {
		calls++
		return ToolResult{Output: json.RawMessage(`{"sent":true}`)}, nil
	}
	if _, err := engine.Tool(context.Background(), "run", "tool", request, invoke); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Tool(context.Background(), "run", "tool", request, invoke); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("side effect ran %d times", calls)
	}
}

func TestApprovalWaitAndResolve(t *testing.T) {
	engine := NewEngine(NewMemoryStore())
	ctx := context.Background()
	if _, err := engine.Approval(ctx, "run", "approve"); !errors.Is(err, ErrApprovalPending) {
		t.Fatalf("expected pending, got %v", err)
	}
	if err := engine.ResolveApproval(ctx, "run", "approve", true, "alice"); err != nil {
		t.Fatal(err)
	}
	approved, err := engine.Approval(ctx, "run", "approve")
	if err != nil || !approved {
		t.Fatalf("unexpected approval approved=%v err=%v", approved, err)
	}
}

func TestBudgetCheckpoint(t *testing.T) {
	engine := NewEngine(NewMemoryStore())
	ctx := context.Background()
	if err := engine.CheckBudget(ctx, "run", "budget-ok", Usage{TotalTokens: 90, CostUSD: 0.09}, Budget{MaxTokens: 100, MaxCostUSD: 0.10}); err != nil {
		t.Fatal(err)
	}
	if err := engine.CheckBudget(ctx, "run", "budget-bad", Usage{TotalTokens: 101}, Budget{MaxTokens: 100}); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected budget error, got %v", err)
	}
}

func TestDurableSleepCompletesAndReplays(t *testing.T) {
	engine := NewEngine(NewMemoryStore())
	ctx := context.Background()
	if err := engine.SleepUntil(ctx, "run", "sleep", time.Now().Add(-time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if err := engine.SleepUntil(ctx, "run", "sleep", time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("checkpointed sleep should return immediately: %v", err)
	}
}
