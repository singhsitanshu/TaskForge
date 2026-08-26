package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge/scheduler/internal/domain"
)

type Postgres struct {
	pool *pgxpool.Pool
}

type expiredTask struct {
	id               string
	workerID         string
	attemptNumber    int16
	maxAttempts      int16
	leaseExpiresAt   time.Time
	recoveredAt      time.Time
	recoveryLagNanos int64
}

type activeAttempt struct {
	id            string
	workerID      string
	attemptNumber int16
	status        string
}

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (repository *Postgres) CollectSnapshot(
	ctx context.Context,
	staleAfter time.Duration,
	deadAfter time.Duration,
) (domain.GlobalSnapshot, error) {
	connection, err := repository.pool.Acquire(ctx)
	if err != nil {
		return domain.GlobalSnapshot{}, fmt.Errorf("acquire snapshot connection: %w", err)
	}
	defer connection.Release()

	snapshot := domain.GlobalSnapshot{
		TaskCounts:   make(map[string]int64),
		WorkerCounts: make(map[string]int64),
	}
	taskRows, err := connection.Query(ctx, `
		SELECT status::text, count(*)
		FROM tasks
		GROUP BY status
	`)
	if err != nil {
		return domain.GlobalSnapshot{}, fmt.Errorf("collect task status counts: %w", err)
	}
	for taskRows.Next() {
		var status string
		var count int64
		if err := taskRows.Scan(&status, &count); err != nil {
			taskRows.Close()
			return domain.GlobalSnapshot{}, fmt.Errorf("scan task status count: %w", err)
		}
		snapshot.TaskCounts[status] = count
	}
	if err := taskRows.Err(); err != nil {
		taskRows.Close()
		return domain.GlobalSnapshot{}, fmt.Errorf("read task status counts: %w", err)
	}
	taskRows.Close()

	workerRows, err := connection.Query(ctx, `
		SELECT liveness, count(*)
		FROM (
			SELECT CASE
				WHEN last_seen_at IS NULL
				  OR last_seen_at <= clock_timestamp() - ($2 * interval '1 microsecond')
					THEN 'DEAD'
				WHEN last_seen_at <= clock_timestamp() - ($1 * interval '1 microsecond')
					THEN 'STALE'
				ELSE 'ACTIVE'
			END AS liveness
			FROM workers
		) AS classified
		GROUP BY liveness
	`, staleAfter.Microseconds(), deadAfter.Microseconds())
	if err != nil {
		return domain.GlobalSnapshot{}, fmt.Errorf("collect worker liveness counts: %w", err)
	}
	for workerRows.Next() {
		var liveness string
		var count int64
		if err := workerRows.Scan(&liveness, &count); err != nil {
			workerRows.Close()
			return domain.GlobalSnapshot{}, fmt.Errorf("scan worker liveness count: %w", err)
		}
		snapshot.WorkerCounts[liveness] = count
	}
	if err := workerRows.Err(); err != nil {
		workerRows.Close()
		return domain.GlobalSnapshot{}, fmt.Errorf("read worker liveness counts: %w", err)
	}
	workerRows.Close()

	err = connection.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM task_attempts WHERE status = 'RUNNING'),
			(SELECT count(*) FROM tasks
			 WHERE status = 'QUEUED'
			   AND scheduled_at <= clock_timestamp()
			   AND attempt_count < max_attempts),
			(SELECT count(*) FROM tasks
			 WHERE status = 'RUNNING'
			   AND lease_expires_at <= clock_timestamp())
	`).Scan(
		&snapshot.RunningAttempts,
		&snapshot.EligibleTasks,
		&snapshot.ExpiredRunningLeases,
	)
	if err != nil {
		return domain.GlobalSnapshot{}, fmt.Errorf("collect operational counts: %w", err)
	}
	return snapshot, nil
}

func (repository *Postgres) PromoteDueRetries(
	ctx context.Context,
	batchSize int,
) ([]domain.PromotedTask, error) {
	tx, err := repository.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin retry promotion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const query = `
		WITH due AS MATERIALIZED (
			SELECT id
			FROM tasks
			WHERE status = 'RETRYING'
			  AND scheduled_at <= clock_timestamp()
			ORDER BY scheduled_at ASC, id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE tasks AS task
		SET
			status = 'QUEUED',
			queued_at = clock_timestamp()
		FROM due
		WHERE task.id = due.id
		  AND task.status = 'RETRYING'
		RETURNING
			task.id::text,
			task.attempt_count,
			task.scheduled_at,
			(EXTRACT(EPOCH FROM (clock_timestamp() - task.scheduled_at)) * 1000000000)::bigint
	`
	rows, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("promote due retries: %w", err)
	}
	promoted := make([]domain.PromotedTask, 0, batchSize)
	for rows.Next() {
		var task domain.PromotedTask
		var latenessNanos int64
		if err := rows.Scan(
			&task.TaskID,
			&task.AttemptNumber,
			&task.ScheduledAt,
			&latenessNanos,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan promoted retry: %w", err)
		}
		task.Lateness = time.Duration(latenessNanos)
		promoted = append(promoted, task)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("read promoted retries: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit retry promotion: %w", err)
	}
	return promoted, nil
}

func (repository *Postgres) RecoverExpired(
	ctx context.Context,
	batchSize int,
) (domain.RecoveryBatch, error) {
	tx, err := repository.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.RecoveryBatch{}, fmt.Errorf("begin recovery transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	tasks, err := lockExpiredTasks(ctx, tx, batchSize)
	if err != nil {
		return domain.RecoveryBatch{}, err
	}

	batch := domain.RecoveryBatch{
		Recovered:  make([]domain.RecoveredTask, 0, len(tasks)),
		Violations: make([]domain.InvariantViolation, 0),
	}
	for _, task := range tasks {
		attempt, attemptErr := lockActiveAttempt(ctx, tx, task)
		if attemptErr != nil {
			if errors.Is(attemptErr, pgx.ErrNoRows) {
				batch.Violations = append(batch.Violations, violation(task, "active_attempt_missing"))
				continue
			}
			return domain.RecoveryBatch{}, attemptErr
		}
		if attempt.workerID != task.workerID {
			batch.Violations = append(batch.Violations, violation(task, "active_attempt_worker_mismatch"))
			continue
		}
		if attempt.attemptNumber != task.attemptNumber {
			batch.Violations = append(batch.Violations, violation(task, "active_attempt_number_mismatch"))
			continue
		}
		if attempt.status != "RUNNING" {
			batch.Violations = append(batch.Violations, violation(task, "active_attempt_not_running"))
			continue
		}

		action := domain.RecoveryRequeued
		if task.attemptNumber >= task.maxAttempts {
			action = domain.RecoveryFailed
		}
		if err := abandonAttempt(ctx, tx, task, attempt, action); err != nil {
			return domain.RecoveryBatch{}, err
		}
		totalLatency, err := transitionTask(ctx, tx, task, action)
		if err != nil {
			return domain.RecoveryBatch{}, err
		}
		batch.Recovered = append(batch.Recovered, domain.RecoveredTask{
			TaskID:         task.id,
			OldWorkerID:    task.workerID,
			AttemptNumber:  task.attemptNumber,
			LeaseExpiresAt: task.leaseExpiresAt,
			RecoveredAt:    task.recoveredAt,
			RecoveryLag:    time.Duration(task.recoveryLagNanos),
			Action:         action,
			TotalLatency:   totalLatency,
		})
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.RecoveryBatch{}, fmt.Errorf("commit recovery transaction: %w", err)
	}
	return batch, nil
}

func lockExpiredTasks(ctx context.Context, tx pgx.Tx, batchSize int) ([]expiredTask, error) {
	const query = `
		WITH recovery_clock AS (
			SELECT clock_timestamp() AS now
		)
		SELECT
			task.id::text,
			task.claimed_by_worker_id::text,
			task.attempt_count,
			task.max_attempts,
			task.lease_expires_at,
			recovery_clock.now,
			(EXTRACT(EPOCH FROM (recovery_clock.now - task.lease_expires_at)) * 1000000000)::bigint
		FROM tasks AS task
		CROSS JOIN recovery_clock
		WHERE task.status = 'RUNNING'
		  AND task.lease_expires_at <= clock_timestamp()
		ORDER BY task.lease_expires_at ASC
		FOR UPDATE OF task SKIP LOCKED
		LIMIT $1
	`
	rows, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("lock expired tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]expiredTask, 0, batchSize)
	for rows.Next() {
		var task expiredTask
		if err := rows.Scan(
			&task.id,
			&task.workerID,
			&task.attemptNumber,
			&task.maxAttempts,
			&task.leaseExpiresAt,
			&task.recoveredAt,
			&task.recoveryLagNanos,
		); err != nil {
			return nil, fmt.Errorf("scan expired task: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read expired tasks: %w", err)
	}
	return tasks, nil
}

func lockActiveAttempt(
	ctx context.Context,
	tx pgx.Tx,
	task expiredTask,
) (activeAttempt, error) {
	const query = `
		SELECT id::text, worker_id::text, attempt_number, status::text
		FROM task_attempts
		WHERE task_id = $1::uuid
		  AND attempt_number = $2
		FOR UPDATE
	`
	var attempt activeAttempt
	if err := tx.QueryRow(ctx, query, task.id, task.attemptNumber).Scan(
		&attempt.id,
		&attempt.workerID,
		&attempt.attemptNumber,
		&attempt.status,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return activeAttempt{}, pgx.ErrNoRows
		}
		return activeAttempt{}, fmt.Errorf("lock active attempt for task %s: %w", task.id, err)
	}
	return attempt, nil
}

func abandonAttempt(
	ctx context.Context,
	tx pgx.Tx,
	task expiredTask,
	attempt activeAttempt,
	action domain.RecoveryAction,
) error {
	const query = `
		UPDATE task_attempts
		SET
			status = 'ABANDONED',
			finished_at = GREATEST(clock_timestamp(), leased_at, started_at),
			output = NULL,
			error = $5,
			recovered_lease_expires_at = $6,
			recovered_at = $7,
			recovery_action = $8
		WHERE id = $1::uuid
		  AND task_id = $2::uuid
		  AND worker_id = $3::uuid
		  AND attempt_number = $4
		  AND status = 'RUNNING'
	`
	commandTag, err := tx.Exec(
		ctx,
		query,
		attempt.id,
		task.id,
		task.workerID,
		task.attemptNumber,
		domain.AttemptAbandonReason,
		task.leaseExpiresAt,
		task.recoveredAt,
		action,
	)
	if err != nil {
		return fmt.Errorf("abandon attempt for task %s: %w", task.id, err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("abandon attempt for task %s: active attempt changed", task.id)
	}
	return nil
}

func transitionTask(
	ctx context.Context,
	tx pgx.Tx,
	task expiredTask,
	action domain.RecoveryAction,
) (time.Duration, error) {
	status := "QUEUED"
	lastError := domain.AttemptAbandonReason
	completionExpression := "NULL"
	if action == domain.RecoveryFailed {
		status = "FAILED"
		lastError = domain.MaxAttemptsExpiredError
		completionExpression = "GREATEST(clock_timestamp(), created_at)"
	}
	query := fmt.Sprintf(`
		UPDATE tasks
		SET
			status = $5::task_status,
			queued_at = CASE
				WHEN $5::task_status = 'QUEUED' THEN $7
				ELSE queued_at
			END,
			claimed_by_worker_id = NULL,
			lease_expires_at = NULL,
			completed_at = %s,
			result = NULL,
			last_error = $6
		WHERE id = $1::uuid
		  AND status = 'RUNNING'
		  AND claimed_by_worker_id = $2::uuid
		  AND attempt_count = $3
		  AND lease_expires_at = $4
		  AND lease_expires_at <= clock_timestamp()
		RETURNING COALESCE(EXTRACT(EPOCH FROM (completed_at - created_at)), 0)
	`, completionExpression)
	var totalLatencySeconds float64
	err := tx.QueryRow(
		ctx,
		query,
		task.id,
		task.workerID,
		task.attemptNumber,
		task.leaseExpiresAt,
		status,
		lastError,
		task.recoveredAt,
	).Scan(&totalLatencySeconds)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("transition recovered task %s: ownership changed", task.id)
		}
		return 0, fmt.Errorf("transition recovered task %s: %w", task.id, err)
	}
	return time.Duration(totalLatencySeconds * float64(time.Second)), nil
}

func violation(task expiredTask, reason string) domain.InvariantViolation {
	return domain.InvariantViolation{
		TaskID:         task.id,
		OldWorkerID:    task.workerID,
		AttemptNumber:  task.attemptNumber,
		LeaseExpiresAt: task.leaseExpiresAt,
		Reason:         reason,
	}
}
