package handler

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"taskforge/worker/internal/domain"
)

type Handler func(
	context.Context,
	json.RawMessage,
	domain.ExecutionMetadata,
) (map[string]any, error)

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	registry := &Registry{handlers: make(map[string]Handler)}
	registry.Register("test.noop", noop)
	registry.Register("test.cpu", cpu)
	registry.Register("test.echo", echo)
	registry.Register("test.fail", fail)
	registry.Register("test.fail_terminal", fail)
	registry.Register("test.fail_retryable", failRetryable)
	registry.Register("test.fail_n_then_succeed", failNThenSucceed)
	registry.Register("test.mixed_failure", mixedFailure)
	registry.Register("test.sleep", sleep)
	return registry
}

func noop(_ context.Context, _ json.RawMessage, _ domain.ExecutionMetadata) (map[string]any, error) {
	return map[string]any{"ok": true}, nil
}

func cpu(ctx context.Context, payload json.RawMessage, _ domain.ExecutionMetadata) (map[string]any, error) {
	var input struct {
		Iterations int `json:"iterations"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Errorf("decode test.cpu payload: %w", err)
	}
	if input.Iterations < 1 || input.Iterations > 10_000_000 {
		return nil, errors.New("test.cpu iterations must be between 1 and 10000000")
	}

	digest := sha256.Sum256([]byte("taskforge-benchmark"))
	for iteration := 0; iteration < input.Iterations; iteration++ {
		if iteration%4096 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		digest = sha256.Sum256(digest[:])
	}
	return map[string]any{
		"iterations": input.Iterations,
		"digest":     fmt.Sprintf("%x", digest),
	}, nil
}

func (r *Registry) Register(taskType string, handler Handler) {
	r.handlers[taskType] = handler
}

func (r *Registry) Execute(
	ctx context.Context,
	taskType string,
	payload json.RawMessage,
	metadata domain.ExecutionMetadata,
) (map[string]any, error) {
	handler, ok := r.handlers[taskType]
	if !ok {
		return nil, fmt.Errorf("no registered handler for task type %q", taskType)
	}
	return handler(ctx, payload, metadata)
}

func echo(_ context.Context, payload json.RawMessage, _ domain.ExecutionMetadata) (map[string]any, error) {
	var input map[string]any
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Errorf("decode test.echo payload: %w", err)
	}
	return map[string]any{"echo": input}, nil
}

func fail(_ context.Context, _ json.RawMessage, _ domain.ExecutionMetadata) (map[string]any, error) {
	return nil, errors.New("test.fail handler requested failure")
}

func failRetryable(_ context.Context, _ json.RawMessage, _ domain.ExecutionMetadata) (map[string]any, error) {
	return nil, domain.Retryable(errors.New("test.fail_retryable handler requested retry"))
}

func failNThenSucceed(
	_ context.Context,
	payload json.RawMessage,
	metadata domain.ExecutionMetadata,
) (map[string]any, error) {
	var input struct {
		Failures int `json:"failures"`
	}
	if err := json.Unmarshal(payload, &input); err != nil {
		return nil, fmt.Errorf("decode test.fail_n_then_succeed payload: %w", err)
	}
	if input.Failures < 0 || input.Failures > 99 {
		return nil, errors.New("test.fail_n_then_succeed failures must be between 0 and 99")
	}
	if int(metadata.AttemptNumber) <= input.Failures {
		return nil, domain.Retryable(fmt.Errorf(
			"test.fail_n_then_succeed retryable failure on attempt %d",
			metadata.AttemptNumber,
		))
	}
	return map[string]any{"succeeded_on_attempt": metadata.AttemptNumber}, nil
}

func mixedFailure(
	ctx context.Context,
	_ json.RawMessage,
	metadata domain.ExecutionMetadata,
) (map[string]any, error) {
	switch metadata.AttemptNumber {
	case 1, 3:
		return nil, domain.Retryable(fmt.Errorf(
			"test.mixed_failure retryable failure on attempt %d",
			metadata.AttemptNumber,
		))
	case 2:
		timer := time.NewTimer(60 * time.Second)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, errors.New("test.mixed_failure attempt 2 was not abandoned")
		}
	default:
		return map[string]any{"succeeded_on_attempt": metadata.AttemptNumber}, nil
	}
}

func sleep(ctx context.Context, payload json.RawMessage, _ domain.ExecutionMetadata) (map[string]any, error) {
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
