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
	id             string
	workerID       string
	attemptNumber  int16
	maxAttempts    int16
	leaseExpiresAt time.Time
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
		SET status = 'QUEUED'
		FROM due
		WHERE task.id = due.id
		  AND task.status = 'RETRYING'
		RETURNING task.id::text, task.attempt_count, task.scheduled_at
	`
	rows, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("promote due retries: %w", err)
	}
	promoted := make([]domain.PromotedTask, 0, batchSize)
	for rows.Next() {
		var task domain.PromotedTask
		if err := rows.Scan(&task.TaskID, &task.AttemptNumber, &task.ScheduledAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan promoted retry: %w", err)
		}
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

		if err := abandonAttempt(ctx, tx, task, attempt); err != nil {
			return domain.RecoveryBatch{}, err
		}
		action := domain.RecoveryRequeued
		if task.attemptNumber >= task.maxAttempts {
			action = domain.RecoveryFailed
		}
		if err := transitionTask(ctx, tx, task, action); err != nil {
			return domain.RecoveryBatch{}, err
		}
		batch.Recovered = append(batch.Recovered, domain.RecoveredTask{
			TaskID:         task.id,
			OldWorkerID:    task.workerID,
			AttemptNumber:  task.attemptNumber,
			LeaseExpiresAt: task.leaseExpiresAt,
			Action:         action,
		})
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.RecoveryBatch{}, fmt.Errorf("commit recovery transaction: %w", err)
	}
	return batch, nil
}

func lockExpiredTasks(ctx context.Context, tx pgx.Tx, batchSize int) ([]expiredTask, error) {
	const query = `
		SELECT
			task.id::text,
			task.claimed_by_worker_id::text,
			task.attempt_count,
			task.max_attempts,
			task.lease_expires_at
		FROM tasks AS task
		WHERE task.status = 'RUNNING'
		  AND task.lease_expires_at <= clock_timestamp()
		ORDER BY task.lease_expires_at ASC
		FOR UPDATE SKIP LOCKED
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
) error {
	const query = `
		UPDATE task_attempts
		SET
			status = 'ABANDONED',
			finished_at = clock_timestamp(),
			output = NULL,
			error = $5
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
) error {
	status := "QUEUED"
	lastError := domain.AttemptAbandonReason
	completionExpression := "NULL"
	if action == domain.RecoveryFailed {
		status = "FAILED"
		lastError = domain.MaxAttemptsExpiredError
		completionExpression = "clock_timestamp()"
	}
	query := fmt.Sprintf(`
		UPDATE tasks
		SET
			status = $5,
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
	`, completionExpression)
	commandTag, err := tx.Exec(
		ctx,
		query,
		task.id,
		task.workerID,
		task.attemptNumber,
		task.leaseExpiresAt,
		status,
		lastError,
	)
	if err != nil {
		return fmt.Errorf("transition recovered task %s: %w", task.id, err)
	}
	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf("transition recovered task %s: ownership changed", task.id)
	}
	return nil
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
