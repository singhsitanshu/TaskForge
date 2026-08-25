package handler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"taskforge/worker/internal/domain"
)

func TestEchoHandler(t *testing.T) {
	result, err := NewRegistry().Execute(
		context.Background(),
		"test.echo",
		json.RawMessage("{\"message\":\"hello\"}"),
		domain.ExecutionMetadata{},
	)
	if err != nil {
		t.Fatalf("execute test.echo: %v", err)
	}

	echoed := result["echo"].(map[string]any)
	if echoed["message"] != "hello" {
		t.Fatalf("expected echoed message, got %#v", result)
	}
}

func TestFailHandler(t *testing.T) {
	_, err := NewRegistry().Execute(context.Background(), "test.fail", json.RawMessage("{}"), domain.ExecutionMetadata{})
	if err == nil || !strings.Contains(err.Error(), "requested failure") {
		t.Fatalf("expected predefined failure, got %v", err)
	}
}

func TestUnknownHandler(t *testing.T) {
	_, err := NewRegistry().Execute(
		context.Background(),
		"shell.command",
		json.RawMessage("{}"),
		domain.ExecutionMetadata{},
	)
	if err == nil || !strings.Contains(err.Error(), "no registered handler") {
		t.Fatalf("expected unregistered handler error, got %v", err)
	}
}

func TestSleepHandler(t *testing.T) {
	result, err := NewRegistry().Execute(
		context.Background(),
		"test.sleep",
		json.RawMessage(`{"duration_ms": 1}`),
		domain.ExecutionMetadata{},
	)
	if err != nil {
		t.Fatalf("execute test.sleep: %v", err)
	}
	if result["slept_ms"] != 1 {
		t.Fatalf("unexpected sleep result: %#v", result)
	}
}

func TestSleepHandlerHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewRegistry().Execute(ctx, "test.sleep", json.RawMessage(`{"duration_ms": 1000}`), domain.ExecutionMetadata{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestRetryableAndAttemptAwareHandlers(t *testing.T) {
	registry := NewRegistry()
	_, err := registry.Execute(
		context.Background(),
		"test.fail_retryable",
		json.RawMessage("{}"),
		domain.ExecutionMetadata{AttemptNumber: 1},
	)
	if !domain.IsRetryable(err) {
		t.Fatalf("failure was not classified retryable: %v", err)
	}

	for attemptNumber := int16(1); attemptNumber <= 3; attemptNumber++ {
		result, err := registry.Execute(
			context.Background(),
			"test.fail_n_then_succeed",
			json.RawMessage(`{"failures": 2}`),
			domain.ExecutionMetadata{AttemptNumber: attemptNumber},
		)
		if attemptNumber <= 2 && !domain.IsRetryable(err) {
			t.Fatalf("attempt %d should retry: result=%v error=%v", attemptNumber, result, err)
		}
		if attemptNumber == 3 && (err != nil || result["succeeded_on_attempt"] != int16(3)) {
			t.Fatalf("attempt 3 should succeed: result=%v error=%v", result, err)
		}
	}
}
