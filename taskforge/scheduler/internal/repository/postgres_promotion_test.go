package repository_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"taskforge/scheduler/internal/domain"
	"taskforge/scheduler/internal/repository"
)

type promotionResult struct {
	tasks []domain.PromotedTask
	err   error
}

func TestRetryPromotionDueBoundaryAndNoAttemptCreation(t *testing.T) {
	database := newTestDatabase(t)
	dueID := insertRetryingTask(t, database, 1, 0)
	futureID := insertRetryingTask(t, database, 1, time.Minute)

	promoted, err := database.store.PromoteDueRetries(context.Background(), 100)
	if err != nil {
		t.Fatalf("promote due retry: %v", err)
	}
	if len(promoted) != 1 || promoted[0].TaskID != dueID || promoted[0].AttemptNumber != 1 {
		t.Fatalf("unexpected promoted retries: %+v", promoted)
	}
	var dueStatus, futureStatus string
	var dueAttempts, futureAttempts int
	err = database.pool.QueryRow(context.Background(), `
		SELECT
			(SELECT status::text FROM tasks WHERE id = $1::uuid),
			(SELECT count(*) FROM task_attempts WHERE task_id = $1::uuid),
			(SELECT status::text FROM tasks WHERE id = $2::uuid),
			(SELECT count(*) FROM task_attempts WHERE task_id = $2::uuid)
	`, dueID, futureID).Scan(&dueStatus, &dueAttempts, &futureStatus, &futureAttempts)
	if err != nil {
		t.Fatalf("read promotion states: %v", err)
	}
	if dueStatus != "QUEUED" || dueAttempts != 0 || futureStatus != "RETRYING" || futureAttempts != 0 {
		t.Fatalf(
			"due=%s/%d future=%s/%d",
			dueStatus, dueAttempts, futureStatus, futureAttempts,
		)
	}
}

func TestRetryPromotionContention(t *testing.T) {
	database := newTestDatabase(t)
	insertRetryingTasks(t, database, 500, -time.Second, "due-retry")
	results := promoteSimultaneously(context.Background(), database.store, 10, 100)
	total := 0
	for _, result := range results {
		if result.err != nil {
			t.Fatalf("promote retry batch: %v", result.err)
		}
		total += len(result.tasks)
	}
	if total != 500 {
		t.Fatalf("promoted=%d expected=500", total)
	}
	var queued, attempts int
	if err := database.pool.QueryRow(context.Background(), `
		SELECT
			count(*) FILTER (WHERE status = 'QUEUED'),
			(SELECT count(*) FROM task_attempts)
		FROM tasks
	`).Scan(&queued, &attempts); err != nil {
		t.Fatalf("read promotion metrics: %v", err)
	}
	if queued != 500 || attempts != 0 {
		t.Fatalf("queued=%d attempts=%d", queued, attempts)
	}
	t.Log("RETRY_PROMOTION_CONTENTION due=500 scanners=10 promoted=500 duplicates=0 attempts_created=0")
}

func TestRetryPromotionQueryUsesIndex(t *testing.T) {
	database := newTestDatabase(t)
	insertRetryingTasks(t, database, 20000, -time.Second, "query-plan-retry")
	if _, err := database.pool.Exec(context.Background(), "ANALYZE tasks"); err != nil {
		t.Fatalf("analyze retry tasks: %v", err)
	}
	rows, err := database.pool.Query(context.Background(), `
		EXPLAIN (ANALYZE, BUFFERS)
		SELECT id
		FROM tasks
		WHERE status = 'RETRYING'
		  AND scheduled_at <= clock_timestamp()
		ORDER BY scheduled_at ASC, id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 100
	`)
	if err != nil {
		t.Fatalf("explain retry promotion: %v", err)
	}
	defer rows.Close()
	lines := make([]string, 0)
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan retry plan: %v", err)
		}
		lines = append(lines, line)
	}
	plan := strings.Join(lines, "\n")
	if !strings.Contains(plan, "tasks_retry_due_idx") {
		t.Fatalf("retry promotion did not use index:\n%s", plan)
	}
	t.Logf("RETRY_PROMOTION_QUERY_PLAN\n%s", plan)
}

func insertRetryingTask(
	t *testing.T,
	database *testDatabase,
	attemptCount int16,
	scheduleOffset time.Duration,
) string {
	t.Helper()
	var taskID string
	if err := database.pool.QueryRow(context.Background(), `
		INSERT INTO tasks (
			task_type, status, attempt_count, max_attempts,
			scheduled_at, last_error
		)
		VALUES (
			'test.fail_retryable', 'RETRYING', $1, 3,
			clock_timestamp() + $2 * interval '1 microsecond', 'temporary failure'
		)
		RETURNING id::text
	`, attemptCount, scheduleOffset.Microseconds()).Scan(&taskID); err != nil {
		t.Fatalf("insert retrying task: %v", err)
	}
	return taskID
}

func insertRetryingTasks(
	t *testing.T,
	database *testDatabase,
	count int,
	scheduleOffset time.Duration,
	taskType string,
) {
	t.Helper()
	if _, err := database.pool.Exec(context.Background(), `
		INSERT INTO tasks (
			task_type, status, attempt_count, max_attempts,
			scheduled_at, last_error
		)
		SELECT
			$1, 'RETRYING', 1, 3,
			clock_timestamp() + $2 * interval '1 microsecond', 'temporary failure'
		FROM generate_series(1, $3)
	`, taskType, scheduleOffset.Microseconds(), count); err != nil {
		t.Fatalf("insert %d retrying tasks: %v", count, err)
	}
}

func promoteSimultaneously(
	ctx context.Context,
	store *repository.Postgres,
	scanners int,
	batchSize int,
) []promotionResult {
	ready := make(chan struct{}, scanners)
	start := make(chan struct{})
	results := make(chan promotionResult, scanners)
	for range scanners {
		go func() {
			ready <- struct{}{}
			<-start
			tasks, err := store.PromoteDueRetries(ctx, batchSize)
			results <- promotionResult{tasks: tasks, err: err}
		}()
	}
	for range scanners {
		<-ready
	}
	close(start)
	promoted := make([]promotionResult, 0, scanners)
	for range scanners {
		promoted = append(promoted, <-results)
	}
	return promoted
}
