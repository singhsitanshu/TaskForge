package repository_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"taskforge/worker/internal/domain"
)

func TestClaimCreatesAtomicFutureLease(t *testing.T) {
	database := newTestDatabase(t)
	worker := registerWorkers(t, database, 1)[0]
	taskID := insertTask(t, database, 0, time.Time{}, "")
	claimed, err := database.store.ClaimNext(context.Background(), worker.ID, 2*time.Second)
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if claimed == nil || claimed.ID != taskID || claimed.AttemptNumber != 1 {
		t.Fatalf("unexpected claim: %#v", claimed)
	}

	var status, owner string
	var attemptCount, attempts int
	var leaseExpiresAt, databaseNow time.Time
	err = database.pool.QueryRow(
		context.Background(),
		`SELECT status::text, claimed_by_worker_id::text, attempt_count, lease_expires_at, clock_timestamp(), (SELECT count(*) FROM task_attempts WHERE task_id = tasks.id) FROM tasks WHERE id = $1::uuid`,
		taskID,
	).Scan(&status, &owner, &attemptCount, &leaseExpiresAt, &databaseNow, &attempts)
	if err != nil {
		t.Fatalf("read claimed task: %v", err)
	}
	if status != "RUNNING" || owner != worker.ID || attemptCount != 1 || attempts != 1 {
		t.Fatalf("invalid ownership status=%s owner=%s count=%d attempts=%d", status, owner, attemptCount, attempts)
	}
	if !leaseExpiresAt.After(databaseNow) || !claimed.LeaseExpiresAt.Equal(leaseExpiresAt) {
		t.Fatalf("invalid lease claimed=%s persisted=%s now=%s", claimed.LeaseExpiresAt, leaseExpiresAt, databaseNow)
	}
}

func TestLeaseRenewalOwnershipAndTerminalRules(t *testing.T) {
	database := newTestDatabase(t)
	workers := registerWorkers(t, database, 2)
	ctx := context.Background()
	insertTask(t, database, 0, time.Time{}, "")
	claimed, err := database.store.ClaimNext(ctx, workers[0].ID, 2*time.Second)
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	originalExpiration := claimed.LeaseExpiresAt
	if _, err := database.pool.Exec(ctx, `SELECT pg_sleep(0.01)`); err != nil {
		t.Fatalf("advance database clock: %v", err)
	}
	renewedExpiration, err := database.store.RenewLease(ctx, claimed.ID, workers[0].ID, 1, 2*time.Second)
	if err != nil {
		t.Fatalf("renew correct owner: %v", err)
	}
	if !renewedExpiration.After(originalExpiration) {
		t.Fatalf("lease did not advance: original=%s renewed=%s", originalExpiration, renewedExpiration)
	}
	t.Logf("LEASE_RENEW original=%s renewed=%s", originalExpiration.Format(time.RFC3339Nano), renewedExpiration.Format(time.RFC3339Nano))

	negativeCases := []struct {
		name     string
		taskID   string
		workerID string
		attempt  int16
	}{
		{"wrong worker", claimed.ID, workers[1].ID, 1},
		{"wrong attempt", claimed.ID, workers[0].ID, 2},
		{"unknown task", "00000000-0000-0000-0000-000000000000", workers[0].ID, 1},
	}
	for _, test := range negativeCases {
		t.Run(test.name, func(t *testing.T) {
			_, err := database.store.RenewLease(ctx, test.taskID, test.workerID, test.attempt, 2*time.Second)
			if !errors.Is(err, domain.ErrLeaseLost) {
				t.Fatalf("expected lease loss, got %v", err)
			}
		})
	}

	var attemptCount, attempts int
	if err := database.pool.QueryRow(ctx, `SELECT attempt_count, (SELECT count(*) FROM task_attempts WHERE task_id = tasks.id) FROM tasks WHERE id = $1::uuid`, claimed.ID).Scan(&attemptCount, &attempts); err != nil {
		t.Fatalf("read renewal invariants: %v", err)
	}
	if attemptCount != 1 || attempts != 1 {
		t.Fatalf("renewal changed attempt state count=%d attempts=%d", attemptCount, attempts)
	}

	if err := database.store.Complete(ctx, workers[0].ID, claimed, map[string]any{"ok": true}, nil); err != nil {
		t.Fatalf("complete task: %v", err)
	}
	if _, err := database.store.RenewLease(ctx, claimed.ID, workers[0].ID, 1, 2*time.Second); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("completed task renewed: %v", err)
	}
	var leaseExpiresAt *time.Time
	var owner *string
	if err := database.pool.QueryRow(ctx, `SELECT lease_expires_at, claimed_by_worker_id::text FROM tasks WHERE id = $1::uuid`, claimed.ID).Scan(&leaseExpiresAt, &owner); err != nil {
		t.Fatalf("read terminal lease: %v", err)
	}
	if leaseExpiresAt != nil || owner != nil {
		t.Fatalf("terminal task retained lease=%v owner=%v", leaseExpiresAt, owner)
	}
}

func TestExpiredLeaseAndStaleAttemptCannotComplete(t *testing.T) {
	database := newTestDatabase(t)
	workers := registerWorkers(t, database, 2)
	ctx := context.Background()
	insertTask(t, database, 0, time.Time{}, "")
	oldClaim, err := database.store.ClaimNext(ctx, workers[0].ID, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("claim expiring task: %v", err)
	}
	if _, err := database.pool.Exec(ctx, `SELECT pg_sleep(0.075)`); err != nil {
		t.Fatalf("wait using database clock: %v", err)
	}
	if _, err := database.store.RenewLease(ctx, oldClaim.ID, workers[0].ID, 1, time.Second); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("expired lease renewed: %v", err)
	}
	if err := database.store.Complete(ctx, workers[0].ID, oldClaim, map[string]any{"stale": true}, nil); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("expired completion accepted: %v", err)
	}

	_, err = database.pool.Exec(
		ctx,
		`UPDATE tasks SET claimed_by_worker_id = $2::uuid, attempt_count = 2, lease_expires_at = clock_timestamp() + interval '1 second' WHERE id = $1::uuid`,
		oldClaim.ID,
		workers[1].ID,
	)
	if err != nil {
		t.Fatalf("simulate future owner: %v", err)
	}
	_, err = database.pool.Exec(
		ctx,
		`INSERT INTO task_attempts (task_id, worker_id, attempt_number, status, started_at) VALUES ($1::uuid, $2::uuid, 2, 'RUNNING', clock_timestamp())`,
		oldClaim.ID,
		workers[1].ID,
	)
	if err != nil {
		t.Fatalf("create future attempt: %v", err)
	}
	if err := database.store.Complete(ctx, workers[0].ID, oldClaim, map[string]any{"late": true}, nil); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("old owner completion accepted: %v", err)
	}

	var status, owner string
	var attemptCount int
	if err := database.pool.QueryRow(ctx, `SELECT status::text, claimed_by_worker_id::text, attempt_count FROM tasks WHERE id = $1::uuid`, oldClaim.ID).Scan(&status, &owner, &attemptCount); err != nil {
		t.Fatalf("read future owner: %v", err)
	}
	if status != "RUNNING" || owner != workers[1].ID || attemptCount != 2 {
		t.Fatalf("old completion corrupted new owner status=%s owner=%s attempt=%d", status, owner, attemptCount)
	}
}

func TestFailedAndCancelledTasksCannotRenew(t *testing.T) {
	database := newTestDatabase(t)
	worker := registerWorkers(t, database, 1)[0]
	ctx := context.Background()

	insertTask(t, database, 0, time.Time{}, "")
	failed, err := database.store.ClaimNext(ctx, worker.ID, time.Second)
	if err != nil {
		t.Fatalf("claim failing task: %v", err)
	}
	if err := database.store.Complete(ctx, worker.ID, failed, nil, errors.New("requested failure")); err != nil {
		t.Fatalf("fail task: %v", err)
	}
	if _, err := database.store.RenewLease(ctx, failed.ID, worker.ID, 1, time.Second); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("failed task renewed: %v", err)
	}

	insertTask(t, database, 0, time.Time{}, "")
	cancelled, err := database.store.ClaimNext(ctx, worker.ID, time.Second)
	if err != nil {
		t.Fatalf("claim cancellable task: %v", err)
	}
	_, err = database.pool.Exec(
		ctx,
		`UPDATE tasks SET status = 'CANCELLED', completed_at = clock_timestamp(), claimed_by_worker_id = NULL, lease_expires_at = NULL WHERE id = $1::uuid`,
		cancelled.ID,
	)
	if err != nil {
		t.Fatalf("cancel task fixture: %v", err)
	}
	if _, err := database.store.RenewLease(ctx, cancelled.ID, worker.ID, 1, time.Second); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("cancelled task renewed: %v", err)
	}
}

func TestExpiredRunningLeaseQueryUsesIndex(t *testing.T) {
	database := newTestDatabase(t)
	worker := registerWorkers(t, database, 1)[0]
	ctx := context.Background()
	_, err := database.pool.Exec(
		ctx,
		`INSERT INTO tasks (task_type, status, claimed_by_worker_id, attempt_count, lease_expires_at) SELECT 'test.echo', 'RUNNING', $1::uuid, 1, clock_timestamp() - interval '1 second' FROM generate_series(1, 20000)`,
		worker.ID,
	)
	if err != nil {
		t.Fatalf("insert expired leases: %v", err)
	}
	if _, err := database.pool.Exec(ctx, `ANALYZE tasks`); err != nil {
		t.Fatalf("analyze tasks: %v", err)
	}
	rows, err := database.pool.Query(
		ctx,
		`EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM tasks WHERE status = 'RUNNING' AND lease_expires_at <= clock_timestamp() ORDER BY lease_expires_at LIMIT 100`,
	)
	if err != nil {
		t.Fatalf("explain expired lease query: %v", err)
	}
	defer rows.Close()
	lines := make([]string, 0)
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		lines = append(lines, line)
	}
	plan := strings.Join(lines, "\n")
	if !strings.Contains(plan, "tasks_running_lease_idx") {
		t.Fatalf("expired lease query did not use index:\n%s", plan)
	}
	t.Logf("LEASE_QUERY_PLAN\n%s", plan)
}
