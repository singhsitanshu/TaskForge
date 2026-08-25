package handler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestEchoHandler(t *testing.T) {
	result, err := NewRegistry().Execute(
		context.Background(),
		"test.echo",
		json.RawMessage("{\"message\":\"hello\"}"),
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
	_, err := NewRegistry().Execute(context.Background(), "test.fail", json.RawMessage("{}"))
	if err == nil || !strings.Contains(err.Error(), "requested failure") {
		t.Fatalf("expected predefined failure, got %v", err)
	}
}

func TestUnknownHandler(t *testing.T) {
	_, err := NewRegistry().Execute(
		context.Background(),
		"shell.command",
		json.RawMessage("{}"),
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
	_, err := NewRegistry().Execute(ctx, "test.sleep", json.RawMessage(`{"duration_ms": 1000}`))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}
