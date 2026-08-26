package repository_test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestCollectSnapshotReturnsExactDatabaseState(t *testing.T) {
	database := newTestDatabase(t)
	ctx := context.Background()

	var ownerID string
	err := database.pool.QueryRow(ctx, `
		INSERT INTO workers (instance_id, name, created_at, last_seen_at)
		VALUES ('active-owner', 'active owner', clock_timestamp() - interval '1 hour', clock_timestamp())
		RETURNING id::text
	`).Scan(&ownerID)
	if err != nil {
		t.Fatalf("insert running owner: %v", err)
	}
	_, err = database.pool.Exec(ctx, `
		INSERT INTO workers (instance_id, name, created_at, last_seen_at) VALUES
			('active-2', 'active 2', clock_timestamp() - interval '1 hour', clock_timestamp()),
			('stale-1', 'stale 1', clock_timestamp() - interval '1 hour', clock_timestamp() - interval '20 seconds'),
			('dead-1', 'dead 1', clock_timestamp() - interval '1 hour', clock_timestamp() - interval '40 seconds'),
			('dead-2', 'dead 2', clock_timestamp() - interval '1 hour', clock_timestamp() - interval '1 minute'),
			('dead-3', 'dead 3', clock_timestamp() - interval '1 hour', NULL)
	`)
	if err != nil {
		t.Fatalf("insert liveness fixtures: %v", err)
	}

	_, err = database.pool.Exec(ctx, `
		INSERT INTO tasks (task_type, status, scheduled_at)
		SELECT 'queued-' || value, 'QUEUED', clock_timestamp() - interval '1 second'
		FROM generate_series(1, 10) AS value;

		INSERT INTO tasks (task_type, status, attempt_count, scheduled_at)
		SELECT 'retrying-' || value, 'RETRYING', 1, clock_timestamp() + interval '1 minute'
		FROM generate_series(1, 3) AS value;

		INSERT INTO tasks (task_type, status, completed_at)
		SELECT 'succeeded-' || value, 'SUCCEEDED', clock_timestamp()
		FROM generate_series(1, 2) AS value;

		INSERT INTO tasks (task_type, status, completed_at)
		VALUES ('failed-1', 'FAILED', clock_timestamp());
	`)
	if err != nil {
		t.Fatalf("insert non-running task fixtures: %v", err)
	}

	for index := 1; index <= 4; index++ {
		var taskID string
		err := database.pool.QueryRow(ctx, `
			INSERT INTO tasks (
				task_type, status, claimed_by_worker_id, attempt_count, lease_expires_at
			)
			VALUES ($1, 'RUNNING', $2::uuid, 1, clock_timestamp() + interval '1 minute')
			RETURNING id::text
		`, fmt.Sprintf("running-%d", index), ownerID).Scan(&taskID)
		if err != nil {
			t.Fatalf("insert running task %d: %v", index, err)
		}
		_, err = database.pool.Exec(ctx, `
			INSERT INTO task_attempts (
				task_id, worker_id, attempt_number, status, started_at
			)
			VALUES ($1::uuid, $2::uuid, 1, 'RUNNING', clock_timestamp())
		`, taskID, ownerID)
		if err != nil {
			t.Fatalf("insert running attempt %d: %v", index, err)
		}
	}

	snapshot, err := database.store.CollectSnapshot(
		ctx,
		15*time.Second,
		30*time.Second,
	)
	if err != nil {
		t.Fatalf("collect snapshot: %v", err)
	}
	wantTasks := map[string]int64{
		"QUEUED": 10, "RUNNING": 4, "RETRYING": 3, "SUCCEEDED": 2, "FAILED": 1,
	}
	for status, want := range wantTasks {
		if got := snapshot.TaskCounts[status]; got != want {
			t.Errorf("task %s=%d want=%d", status, got, want)
		}
	}
	wantWorkers := map[string]int64{"ACTIVE": 2, "STALE": 1, "DEAD": 3}
	for liveness, want := range wantWorkers {
		if got := snapshot.WorkerCounts[liveness]; got != want {
			t.Errorf("worker %s=%d want=%d", liveness, got, want)
		}
	}
	if snapshot.RunningAttempts != 4 || snapshot.EligibleTasks != 10 || snapshot.ExpiredRunningLeases != 0 {
		t.Errorf("unexpected operational snapshot: %+v", snapshot)
	}

	_, err = database.pool.Exec(ctx, `
		UPDATE tasks
		SET status = 'CANCELLED', completed_at = clock_timestamp()
		WHERE task_type IN ('queued-1', 'queued-2')
	`)
	if err != nil {
		t.Fatalf("mutate snapshot fixtures: %v", err)
	}
	second, err := database.store.CollectSnapshot(ctx, 15*time.Second, 30*time.Second)
	if err != nil {
		t.Fatalf("collect second snapshot: %v", err)
	}
	if second.TaskCounts["QUEUED"] != 8 || second.TaskCounts["CANCELLED"] != 2 || second.EligibleTasks != 8 {
		t.Fatalf("second snapshot accumulated instead of replacing: %+v", second)
	}
}
