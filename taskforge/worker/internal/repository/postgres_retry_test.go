package repository_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"taskforge/worker/internal/domain"
)

func TestRetryableFailureSchedulesWithoutCreatingAttempt(t *testing.T) {
	database := newTestDatabase(t)
	worker := registerWorkers(t, database, 1)[0]
	taskID := insertTask(t, database, 0, time.Time{}, "")
	claimed, err := database.store.ClaimNext(context.Background(), worker.ID, time.Second)
	if err != nil || claimed == nil {
		t.Fatalf("claim retry task: task=%v error=%v", claimed, err)
	}
	outcome, err := database.store.RetryableFail(
		context.Background(),
		worker.ID,
		claimed,
		errors.New("temporary upstream timeout"),
		200*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("schedule retry: %v", err)
	}
	if outcome.Exhausted || outcome.Delay != 200*time.Millisecond {
		t.Fatalf("unexpected retry outcome: %+v", outcome)
	}

	var status, attemptStatus, attemptError string
	var attemptCount, attempts int
	var owner *string
	var lease *time.Time
	var scheduledAt, databaseNow time.Time
	var finishedAt *time.Time
	err = database.pool.QueryRow(context.Background(), `
		SELECT
			task.status::text,
			task.attempt_count,
			task.claimed_by_worker_id::text,
			task.lease_expires_at,
			task.scheduled_at,
			clock_timestamp(),
			attempt.status::text,
			attempt.error,
			attempt.finished_at,
			(SELECT count(*) FROM task_attempts WHERE task_id = task.id)
		FROM tasks AS task
		JOIN task_attempts AS attempt ON attempt.task_id = task.id
		WHERE task.id = $1::uuid AND attempt.attempt_number = 1
	`, taskID).Scan(
		&status,
		&attemptCount,
		&owner,
		&lease,
		&scheduledAt,
		&databaseNow,
		&attemptStatus,
		&attemptError,
		&finishedAt,
		&attempts,
	)
	if err != nil {
		t.Fatalf("read scheduled retry: %v", err)
	}
	if status != "RETRYING" || attemptCount != 1 || owner != nil || lease != nil ||
		!scheduledAt.After(databaseNow) || attemptStatus != "FAILED" ||
		attemptError != "temporary upstream timeout" || finishedAt == nil || attempts != 1 {
		t.Fatalf(
			"status=%s count=%d owner=%v lease=%v scheduled=%s now=%s attempt=%s error=%s finished=%v attempts=%d",
			status, attemptCount, owner, lease, scheduledAt, databaseNow,
			attemptStatus, attemptError, finishedAt, attempts,
		)
	}
	if !outcome.RetryAt.Equal(scheduledAt) {
		t.Fatalf("returned retry_at=%s persisted=%s", outcome.RetryAt, scheduledAt)
	}
	if next, err := database.store.ClaimNext(context.Background(), worker.ID); err != nil || next != nil {
		t.Fatalf("RETRYING task was claimable: task=%v error=%v", next, err)
	}
}

func TestRetryableFailureExhaustionAndStaleOwner(t *testing.T) {
	database := newTestDatabase(t)
	workers := registerWorkers(t, database, 2)

	exhaustedID := insertTask(t, database, 0, time.Time{}, "")
	if _, err := database.pool.Exec(
		context.Background(),
		"UPDATE tasks SET max_attempts = 1 WHERE id = $1::uuid",
		exhaustedID,
	); err != nil {
		t.Fatalf("set max attempts: %v", err)
	}
	exhaustedClaim, err := database.store.ClaimNext(context.Background(), workers[0].ID, time.Second)
	if err != nil || exhaustedClaim == nil {
		t.Fatalf("claim exhaustion task: task=%v error=%v", exhaustedClaim, err)
	}
	outcome, err := database.store.RetryableFail(
		context.Background(),
		workers[0].ID,
		exhaustedClaim,
		errors.New("still unavailable"),
		100*time.Millisecond,
	)
	if err != nil || !outcome.Exhausted || !outcome.RetryAt.IsZero() {
		t.Fatalf("exhaust retry: outcome=%+v error=%v", outcome, err)
	}
	var taskStatus, attemptStatus string
	var completedAt *time.Time
	if err := database.pool.QueryRow(context.Background(), `
		SELECT task.status::text, task.completed_at, attempt.status::text
		FROM tasks AS task
		JOIN task_attempts AS attempt ON attempt.task_id = task.id
		WHERE task.id = $1::uuid
	`, exhaustedID).Scan(&taskStatus, &completedAt, &attemptStatus); err != nil {
		t.Fatalf("read exhausted retry: %v", err)
	}
	if taskStatus != "FAILED" || completedAt == nil || attemptStatus != "FAILED" {
		t.Fatalf("exhausted state task=%s completed=%v attempt=%s", taskStatus, completedAt, attemptStatus)
	}

	staleID := insertTask(t, database, 0, time.Time{}, "")
	staleClaim, err := database.store.ClaimNext(context.Background(), workers[0].ID, 25*time.Millisecond)
	if err != nil || staleClaim == nil || staleClaim.ID != staleID {
		t.Fatalf("claim stale task: task=%v error=%v", staleClaim, err)
	}
	if _, err := database.pool.Exec(context.Background(), "SELECT pg_sleep(0.04)"); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	if _, err := database.store.RetryableFail(
		context.Background(),
		workers[0].ID,
		staleClaim,
		errors.New("late retry"),
		time.Second,
	); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale owner scheduled retry: %v", err)
	}
	var staleStatus, staleAttemptStatus string
	if err := database.pool.QueryRow(context.Background(), `
		SELECT task.status::text, attempt.status::text
		FROM tasks AS task
		JOIN task_attempts AS attempt ON attempt.task_id = task.id
		WHERE task.id = $1::uuid
	`, staleID).Scan(&staleStatus, &staleAttemptStatus); err != nil {
		t.Fatalf("read stale task: %v", err)
	}
	if staleStatus != "RUNNING" || staleAttemptStatus != "RUNNING" {
		t.Fatalf("stale retry changed task=%s attempt=%s", staleStatus, staleAttemptStatus)
	}
}

func TestRetryableFailureTransactionRollback(t *testing.T) {
	database := newTestDatabase(t)
	worker := registerWorkers(t, database, 1)[0]
	taskID := insertTask(t, database, 0, time.Time{}, "")
	claimed, err := database.store.ClaimNext(context.Background(), worker.ID, time.Second)
	if err != nil || claimed == nil {
		t.Fatalf("claim rollback task: task=%v error=%v", claimed, err)
	}
	_, err = database.pool.Exec(context.Background(), `
		CREATE FUNCTION reject_retry_task_transition()
		RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF OLD.status = 'RUNNING' AND NEW.status IN ('RETRYING', 'FAILED') THEN
				RAISE EXCEPTION 'TF-009 injected retry transition failure';
			END IF;
			RETURN NEW;
		END;
		$$;
		CREATE TRIGGER reject_retry_task_transition
		BEFORE UPDATE ON tasks
		FOR EACH ROW EXECUTE FUNCTION reject_retry_task_transition();
	`)
	if err != nil {
		t.Fatalf("install retry failure trigger: %v", err)
	}
	_, retryErr := database.store.RetryableFail(
		context.Background(),
		worker.ID,
		claimed,
		errors.New("retry me"),
		time.Second,
	)
	if retryErr == nil || !strings.Contains(retryErr.Error(), "injected retry transition failure") {
		t.Fatalf("expected injected retry failure, got %v", retryErr)
	}
	var taskStatus, attemptStatus, owner string
	var lease *time.Time
	if err := database.pool.QueryRow(context.Background(), `
		SELECT task.status::text, task.claimed_by_worker_id::text,
		       task.lease_expires_at, attempt.status::text
		FROM tasks AS task
		JOIN task_attempts AS attempt ON attempt.task_id = task.id
		WHERE task.id = $1::uuid
	`, taskID).Scan(&taskStatus, &owner, &lease, &attemptStatus); err != nil {
		t.Fatalf("read retry rollback: %v", err)
	}
	if taskStatus != "RUNNING" || owner != worker.ID || lease == nil || attemptStatus != "RUNNING" {
		t.Fatalf("rollback state task=%s owner=%s lease=%v attempt=%s", taskStatus, owner, lease, attemptStatus)
	}
}
