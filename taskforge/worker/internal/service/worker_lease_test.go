package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"testing"
	"time"

	"taskforge/worker/internal/domain"
)

type leaseTestStore struct {
	mu         sync.Mutex
	renewCalls int
	failFirst  bool
	loseLease  bool
	completed  bool
}

func (s *leaseTestStore) ClaimNext(context.Context, string, ...time.Duration) (*domain.ClaimedTask, error) {
	return nil, nil
}

func (s *leaseTestStore) RenewLease(_ context.Context, _, _ string, _ int16, duration time.Duration) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewCalls++
	if s.loseLease {
		return time.Time{}, domain.ErrLeaseLost
	}
	if s.failFirst && s.renewCalls == 1 {
		return time.Time{}, errors.New("temporary database failure")
	}
	return time.Now().Add(duration), nil
}

func (s *leaseTestStore) Complete(context.Context, string, *domain.ClaimedTask, map[string]any, error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed = true
	return nil
}

type leaseTestExecutor struct {
	duration  time.Duration
	cancelled chan struct{}
}

func (e *leaseTestExecutor) Execute(ctx context.Context, _ string, _ json.RawMessage) (map[string]any, error) {
	timer := time.NewTimer(e.duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		if e.cancelled != nil {
			close(e.cancelled)
		}
		return nil, ctx.Err()
	case <-timer.C:
		return map[string]any{"ok": true}, nil
	}
}

func TestTransientLeaseRenewalFailureRecovers(t *testing.T) {
	store := &leaseTestStore{failFirst: true}
	var logs bytes.Buffer
	worker := New(store, &leaseTestExecutor{duration: 45 * time.Millisecond}, "worker", "instance", time.Millisecond, 100*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond, log.New(&logs, "", 0))
	worker.executeClaimedTask(context.Background(), &domain.ClaimedTask{ID: "task", AttemptID: "attempt", AttemptNumber: 1, Type: "test", LeaseExpiresAt: time.Now().Add(100 * time.Millisecond)})

	store.mu.Lock()
	renewCalls, completed := store.renewCalls, store.completed
	store.mu.Unlock()
	if renewCalls < 2 || !completed {
		t.Fatalf("renew calls=%d completed=%t", renewCalls, completed)
	}
	if !bytes.Contains(logs.Bytes(), []byte("event=task_lease_renew_failed")) {
		t.Fatalf("missing renewal failure log: %s", logs.String())
	}
}

func TestLeaseLossCancelsHandlerAndSkipsCompletion(t *testing.T) {
	store := &leaseTestStore{loseLease: true}
	cancelled := make(chan struct{})
	var logs bytes.Buffer
	worker := New(store, &leaseTestExecutor{duration: time.Second, cancelled: cancelled}, "worker", "instance", time.Millisecond, 100*time.Millisecond, 10*time.Millisecond, 5*time.Millisecond, log.New(&logs, "", 0))
	worker.executeClaimedTask(context.Background(), &domain.ClaimedTask{ID: "task", AttemptID: "attempt", AttemptNumber: 1, Type: "test", LeaseExpiresAt: time.Now().Add(100 * time.Millisecond)})

	select {
	case <-cancelled:
	default:
		t.Fatal("handler context was not cancelled")
	}
	store.mu.Lock()
	completed := store.completed
	store.mu.Unlock()
	if completed {
		t.Fatal("lease-lost task was completed")
	}
	if !bytes.Contains(logs.Bytes(), []byte("event=task_lease_lost")) {
		t.Fatalf("missing lease loss log: %s", logs.String())
	}
}
