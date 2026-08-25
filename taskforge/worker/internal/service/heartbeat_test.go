package service

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

type recoveringHeartbeatStore struct {
	mu      sync.Mutex
	calls   int
	success chan struct{}
	failed  bool
}

func (s *recoveringHeartbeatStore) Heartbeat(_ context.Context, _, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if !s.failed {
		s.failed = true
		return errors.New("temporary database failure")
	}
	select {
	case s.success <- struct{}{}:
	default:
	}
	return nil
}

func TestHeartbeatFailureDoesNotStopLoop(t *testing.T) {
	store := &recoveringHeartbeatStore{success: make(chan struct{}, 1)}
	var logs bytes.Buffer
	heartbeater := NewHeartbeater(
		store,
		"worker-id",
		"instance-id",
		5*time.Millisecond,
		20*time.Millisecond,
		log.New(&logs, "", 0),
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		heartbeater.Run(ctx)
	}()

	select {
	case <-store.success:
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not retry after failure")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not stop after cancellation")
	}

	store.mu.Lock()
	calls := store.calls
	store.mu.Unlock()
	if calls < 2 {
		t.Fatalf("expected at least two heartbeat calls, got %d", calls)
	}
	if !strings.Contains(logs.String(), "event=heartbeat_failed") {
		t.Fatalf("expected structured failure log, got %q", logs.String())
	}
}
