package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Handler func(context.Context, json.RawMessage) (map[string]any, error)

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	registry := &Registry{handlers: make(map[string]Handler)}
	registry.Register("test.echo", echo)
	registry.Register("test.fail", fail)
	registry.Register("test.sleep", sleep)
	return registry
}

func (r *Registry) Register(taskType string, handler Handler) {
	r.handlers[taskType] = handler
}

func (r *Registry) Execute(
	ctx context.Context,
	taskType string,
	payload json.RawMessage,
) (map[string]any, error) {
	handler, ok := r.handlers[taskType]
	if !ok {
		return nil, fmt.Errorf("no registered handler for task type %q", taskType)
	}
	return handler(ctx, payload)
}

func echo(_ context.Context, payload json.RawMessage) (map[string]any, error) {
	var input map[string]any
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Errorf("decode test.echo payload: %w", err)
	}
	return map[string]any{"echo": input}, nil
}

func fail(_ context.Context, _ json.RawMessage) (map[string]any, error) {
	return nil, errors.New("test.fail handler requested failure")
}

func sleep(ctx context.Context, payload json.RawMessage) (map[string]any, error) {
	var input struct {
		DurationMilliseconds int `json:"duration_ms"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Errorf("decode test.sleep payload: %w", err)
	}
	if input.DurationMilliseconds < 1 || input.DurationMilliseconds > 60_000 {
		return nil, errors.New("test.sleep duration_ms must be between 1 and 60000")
	}
	timer := time.NewTimer(time.Duration(input.DurationMilliseconds) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return map[string]any{"slept_ms": input.DurationMilliseconds}, nil
	}
}
