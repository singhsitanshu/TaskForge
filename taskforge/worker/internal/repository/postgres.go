package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"taskforge/worker/internal/domain"
)

type Postgres struct {
	pool *pgxpool.Pool
}

var ErrWorkerNotFound = errors.New("worker registration not found")

func NewPostgres(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (r *Postgres) RegisterWorker(
	ctx context.Context,
	instanceID string,
	name string,
) (string, error) {
	const query = `
		INSERT INTO workers (instance_id, name, enabled, last_seen_at)
		VALUES ($1, $2, true, clock_timestamp())
		ON CONFLICT (instance_id)
		DO UPDATE SET
			name = EXCLUDED.name,
			enabled = true,
			last_seen_at = clock_timestamp()
		RETURNING id::text
	`

	var workerID string
	if err := r.pool.QueryRow(ctx, query, instanceID, name).Scan(&workerID); err != nil {
		return "", fmt.Errorf("register worker: %w", err)
	}
	return workerID, nil
}

func (r *Postgres) Heartbeat(
	ctx context.Context,
	workerID string,
	instanceID string,
) error {
	const query = `
		UPDATE workers
		SET last_seen_at = clock_timestamp()
		WHERE id = $1::uuid
		  AND instance_id = $2
	`
	commandTag, err := r.pool.Exec(ctx, query, workerID, instanceID)
	if err != nil {
		return fmt.Errorf("update worker heartbeat: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrWorkerNotFound
	}
	return nil
}

func (r *Postgres) ClaimNext(
	ctx context.Context,
	workerID string,
) (*domain.ClaimedTask, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin claim transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	const selectTask = `
		SELECT
			candidate.id::text,
			candidate.task_type,
			candidate.payload,
			(candidate.attempt_count + 1)::smallint
		FROM tasks AS candidate
		WHERE candidate.status = 'QUEUED'
		  AND candidate.scheduled_at <= clock_timestamp()
		  AND candidate.attempt_count < candidate.max_attempts
		ORDER BY
			candidate.priority DESC,
			candidate.created_at ASC,
			candidate.id ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`

	task := &domain.ClaimedTask{}
	if err := tx.QueryRow(ctx, selectTask).Scan(
		&task.ID,
		&task.Type,
		&task.Payload,
		&task.AttemptNumber,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("select queued task: %w", err)
	}

	const claimTask = `
		UPDATE tasks
		SET
			status = 'RUNNING',
			claimed_by_worker_id = $2::uuid,
			attempt_count = $3
		WHERE id = $1::uuid
		  AND status = 'QUEUED'
	`
	commandTag, err := tx.Exec(ctx, claimTask, task.ID, workerID, task.AttemptNumber)
	if err != nil {
		return nil, fmt.Errorf("mark task running: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return nil, errors.New("queued task changed before claim")
	}

	const createAttempt = `
		INSERT INTO task_attempts (
			task_id,
			worker_id,
			attempt_number,
			status,
			started_at
		)
		VALUES ($1::uuid, $2::uuid, $3, 'RUNNING', clock_timestamp())
		RETURNING id::text
	`
	if err := tx.QueryRow(
		ctx,
		createAttempt,
		task.ID,
		workerID,
		task.AttemptNumber,
	).Scan(&task.AttemptID); err != nil {
		return nil, fmt.Errorf("create task attempt: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit task claim: %w", err)
	}
	return task, nil
}

func (r *Postgres) Complete(
	ctx context.Context,
	workerID string,
	task *domain.ClaimedTask,
	result map[string]any,
	executionErr error,
) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin completion transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	const selectState = `
		SELECT status::text, COALESCE(claimed_by_worker_id::text, '')
		FROM tasks
		WHERE id = $1::uuid
		FOR UPDATE
	`
	var taskStatus string
	var claimedBy string
	if err := tx.QueryRow(ctx, selectState, task.ID).Scan(&taskStatus, &claimedBy); err != nil {
		return fmt.Errorf("lock task for completion: %w", err)
	}

	if taskStatus == "CANCELLED" {
		if _, err := tx.Exec(
			ctx,
			`
				UPDATE task_attempts
				SET status = 'CANCELLED', finished_at = clock_timestamp(), error = 'task cancelled'
				WHERE id = $1::uuid AND status = 'RUNNING'
			`,
			task.AttemptID,
		); err != nil {
			return fmt.Errorf("cancel task attempt: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit cancelled attempt: %w", err)
		}
		return nil
	}

	if taskStatus != "RUNNING" || claimedBy != workerID {
		return fmt.Errorf(
			"task %s is not claimed by worker %s (status=%s, claimed_by=%s)",
			task.ID,
			workerID,
			taskStatus,
			claimedBy,
		)
	}

	if executionErr == nil {
		encodedResult, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("encode task result: %w", err)
		}
		if err := completeSuccess(ctx, tx, task, workerID, string(encodedResult)); err != nil {
			return err
		}
	} else if err := completeFailure(
		ctx,
		tx,
		task,
		workerID,
		executionErr.Error(),
	); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit task completion: %w", err)
	}
	return nil
}

func completeSuccess(
	ctx context.Context,
	tx pgx.Tx,
	task *domain.ClaimedTask,
	workerID string,
	result string,
) error {
	commandTag, err := tx.Exec(
		ctx,
		`
			UPDATE task_attempts
			SET
				status = 'SUCCEEDED',
				finished_at = clock_timestamp(),
				output = $2::jsonb,
				error = NULL
			WHERE id = $1::uuid
			  AND status = 'RUNNING'
		`,
		task.AttemptID,
		result,
	)
	if err != nil {
		return fmt.Errorf("mark task attempt succeeded: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return errors.New("task attempt changed before success submission")
	}

	commandTag, err = tx.Exec(
		ctx,
		`
			UPDATE tasks
			SET
				status = 'SUCCEEDED',
				completed_at = clock_timestamp(),
				result = $3::jsonb,
				last_error = NULL,
				claimed_by_worker_id = NULL
			WHERE id = $1::uuid
			  AND status = 'RUNNING'
			  AND claimed_by_worker_id = $2::uuid
		`,
		task.ID,
		workerID,
		result,
	)
	if err != nil {
		return fmt.Errorf("mark task succeeded: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return errors.New("task claim was lost before success submission")
	}
	return nil
}

func completeFailure(
	ctx context.Context,
	tx pgx.Tx,
	task *domain.ClaimedTask,
	workerID string,
	message string,
) error {
	commandTag, err := tx.Exec(
		ctx,
		`
			UPDATE task_attempts
			SET
				status = 'FAILED',
				finished_at = clock_timestamp(),
				output = NULL,
				error = $2
			WHERE id = $1::uuid
			  AND status = 'RUNNING'
		`,
		task.AttemptID,
		message,
	)
	if err != nil {
		return fmt.Errorf("mark task attempt failed: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return errors.New("task attempt changed before failure submission")
	}

	commandTag, err = tx.Exec(
		ctx,
		`
			UPDATE tasks
			SET
				status = 'FAILED',
				completed_at = clock_timestamp(),
				result = NULL,
				last_error = $3,
				claimed_by_worker_id = NULL
			WHERE id = $1::uuid
			  AND status = 'RUNNING'
			  AND claimed_by_worker_id = $2::uuid
		`,
		task.ID,
		workerID,
		message,
	)
	if err != nil {
		return fmt.Errorf("mark task failed: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return errors.New("task claim was lost before failure submission")
	}
	return nil
}
