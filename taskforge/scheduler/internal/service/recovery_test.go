package service

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"taskforge/scheduler/internal/config"
	"taskforge/scheduler/internal/domain"
)

type scriptedStore struct {
	mu      sync.Mutex
	calls   int
	errors  int
	called  chan struct{}
	batches []domain.RecoveryBatch
}

func (store *scriptedStore) RecoverExpired(
	_ context.Context,
	_ int,
) (domain.RecoveryBatch, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.calls++
	select {
	case store.called <- struct{}{}:
	default:
	}
	if store.errors > 0 {
		store.errors--
		return domain.RecoveryBatch{}, errors.New("transient database failure")
	}
	if len(store.batches) == 0 {
		return domain.RecoveryBatch{}, nil
	}
	batch := store.batches[0]
	store.batches = store.batches[1:]
	return batch, nil
}

func TestRecoveryLoopRetriesOnNextIntervalAndStops(t *testing.T) {
	store := &scriptedStore{errors: 1, called: make(chan struct{}, 4)}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	recovery := NewRecovery(store, config.Recovery{
		Interval:  20 * time.Millisecond,
		BatchSize: 10,
		DBTimeout: 10 * time.Millisecond,
	}, logger)

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		recovery.Run(ctx)
	}()
	for range 2 {
		select {
		case <-store.called:
		case <-time.After(time.Second):
			t.Fatal("recovery loop did not scan")
		}
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("recovery loop did not stop with its context")
	}

	store.mu.Lock()
	calls := store.calls
	store.mu.Unlock()
	if calls != 2 {
		t.Fatalf("expected one initial and one ticker scan, got %d", calls)
	}
	if !bytes.Contains(logs.Bytes(), []byte("task_recovery_scan_failed")) {
		t.Fatalf("transient failure was not logged: %s", logs.String())
	}
}

func TestRecoveryLogsDurableOutcomesAndViolations(t *testing.T) {
	store := &scriptedStore{
		called: make(chan struct{}, 1),
		batches: []domain.RecoveryBatch{{
			Recovered: []domain.RecoveredTask{
				{TaskID: "requeued", Action: domain.RecoveryRequeued},
				{TaskID: "failed", Action: domain.RecoveryFailed},
			},
			Violations: []domain.InvariantViolation{{
				TaskID: "corrupt", Reason: "active_attempt_missing",
			}},
		}},
	}
	var logs bytes.Buffer
	recovery := NewRecovery(store, config.Recovery{
		Interval:  time.Hour,
		BatchSize: 100,
		DBTimeout: time.Second,
	}, slog.New(slog.NewJSONHandler(&logs, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		recovery.Run(ctx)
	}()
	<-store.called
	cancel()
	<-done

	for _, event := range []string{
		"task_recovered",
		"task_recovery_exhausted",
		"task_recovery_invariant_violation",
	} {
		if !bytes.Contains(logs.Bytes(), []byte(event)) {
			t.Fatalf("missing %s log: %s", event, logs.String())
		}
	}
}
