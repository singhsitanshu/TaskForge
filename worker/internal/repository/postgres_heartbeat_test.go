package repository_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"taskforge/worker/internal/repository"
)

func TestWorkerRegistrationAndHeartbeatPersistence(t *testing.T) {
	database := newTestDatabase(t)
	ctx := context.Background()

	firstID, err := database.store.RegisterWorker(ctx, "heartbeat-instance-1", "worker one")
	if err != nil {
		t.Fatalf("register first worker: %v", err)
	}
	var registeredAt, firstHeartbeat time.Time
	if err := database.pool.QueryRow(
		ctx,
		`SELECT created_at, last_seen_at FROM workers WHERE id = $1::uuid`,
		firstID,
	).Scan(&registeredAt, &firstHeartbeat); err != nil {
		t.Fatalf("read registered worker: %v", err)
	}
	if firstHeartbeat.Before(registeredAt) {
		t.Fatalf("registration heartbeat %s precedes registration %s", firstHeartbeat, registeredAt)
	}

	if _, err := database.pool.Exec(ctx, `SELECT pg_sleep(0.01)`); err != nil {
		t.Fatalf("advance database clock: %v", err)
	}
	if err := database.store.Heartbeat(ctx, firstID, "heartbeat-instance-1"); err != nil {
		t.Fatalf("heartbeat first worker: %v", err)
	}
	var secondHeartbeat time.Time
	if err := database.pool.QueryRow(
		ctx,
		`SELECT last_seen_at FROM workers WHERE id = $1::uuid`,
		firstID,
	).Scan(&secondHeartbeat); err != nil {
		t.Fatalf("read second heartbeat: %v", err)
	}
	if !secondHeartbeat.After(firstHeartbeat) {
		t.Fatalf("heartbeat did not advance: first=%s second=%s", firstHeartbeat, secondHeartbeat)
	}

	secondID, err := database.store.RegisterWorker(ctx, "heartbeat-instance-2", "worker two")
	if err != nil {
		t.Fatalf("register second worker: %v", err)
	}
	var secondWorkerHeartbeat time.Time
	if err := database.pool.QueryRow(
		ctx,
		`SELECT last_seen_at FROM workers WHERE id = $1::uuid`,
		secondID,
	).Scan(&secondWorkerHeartbeat); err != nil {
		t.Fatalf("read second worker heartbeat: %v", err)
	}
	if _, err := database.pool.Exec(ctx, `SELECT pg_sleep(0.01)`); err != nil {
		t.Fatalf("advance database clock: %v", err)
	}
	if err := database.store.Heartbeat(ctx, firstID, "heartbeat-instance-1"); err != nil {
		t.Fatalf("heartbeat first worker again: %v", err)
	}
	var unchangedSecondWorkerHeartbeat time.Time
	if err := database.pool.QueryRow(
		ctx,
		`SELECT last_seen_at FROM workers WHERE id = $1::uuid`,
		secondID,
	).Scan(&unchangedSecondWorkerHeartbeat); err != nil {
		t.Fatalf("reread second worker heartbeat: %v", err)
	}
	if !unchangedSecondWorkerHeartbeat.Equal(secondWorkerHeartbeat) {
		t.Fatalf("heartbeat updated wrong worker: before=%s after=%s", secondWorkerHeartbeat, unchangedSecondWorkerHeartbeat)
	}

	refreshedID, err := database.store.RegisterWorker(
		ctx,
		"heartbeat-instance-1",
		"worker one renamed",
	)
	if err != nil {
		t.Fatalf("refresh worker registration: %v", err)
	}
	if refreshedID != firstID {
		t.Fatalf("registration created a new worker: first=%s refreshed=%s", firstID, refreshedID)
	}
	var rowCount int
	if err := database.pool.QueryRow(
		ctx,
		`SELECT count(*) FROM workers WHERE instance_id = 'heartbeat-instance-1'`,
	).Scan(&rowCount); err != nil {
		t.Fatalf("count worker rows: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("expected one worker row, got %d", rowCount)
	}

	missingID := "00000000-0000-0000-0000-000000000000"
	if err := database.store.Heartbeat(ctx, missingID, "missing"); !errors.Is(err, repository.ErrWorkerNotFound) {
		t.Fatalf("expected missing worker error, got %v", err)
	}
	if err := database.store.Heartbeat(ctx, firstID, "wrong-instance"); !errors.Is(err, repository.ErrWorkerNotFound) {
		t.Fatalf("expected mismatched identity error, got %v", err)
	}
}
