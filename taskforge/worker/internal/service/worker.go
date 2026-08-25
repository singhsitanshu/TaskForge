package service

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"taskforge/worker/internal/domain"
)

type Store interface {
	ClaimNext(context.Context, string) (*domain.ClaimedTask, error)
	Complete(
		context.Context,
		string,
		*domain.ClaimedTask,
		map[string]any,
		error,
	) error
}

type Executor interface {
	Execute(context.Context, string, json.RawMessage) (map[string]any, error)
}

type Worker struct {
	store        Store
	executor     Executor
	workerID     string
	instanceID   string
	pollInterval time.Duration
	logger       *log.Logger
}

func New(
	store Store,
	executor Executor,
	workerID string,
	instanceID string,
	pollInterval time.Duration,
	logger *log.Logger,
) *Worker {
	return &Worker{
		store:        store,
		executor:     executor,
		workerID:     workerID,
		instanceID:   instanceID,
		pollInterval: pollInterval,
		logger:       logger,
	}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		task, err := w.store.ClaimNext(ctx, w.workerID)
		if err != nil {
			w.logger.Printf("poll queued task: %v", err)
			if !wait(ctx, w.pollInterval) {
				return
			}
			continue
		}
		if task == nil {
			if !wait(ctx, w.pollInterval) {
				return
			}
			continue
		}

		w.logger.Printf(
			"event=task_claimed worker_instance_id=%s task_id=%s attempt_number=%d task_type=%s",
			w.instanceID,
			task.ID,
			task.AttemptNumber,
			task.Type,
		)
		result, executionErr := w.executor.Execute(ctx, task.Type, task.Payload)
		if err := w.store.Complete(
			ctx,
			w.workerID,
			task,
			result,
			executionErr,
		); err != nil {
			w.logger.Printf(
				"event=task_completion_failed worker_instance_id=%s task_id=%s attempt_number=%d error=%q",
				w.instanceID,
				task.ID,
				task.AttemptNumber,
				err,
			)
			if !wait(ctx, w.pollInterval) {
				return
			}
			continue
		}
		if executionErr != nil {
			w.logger.Printf(
				"event=task_failed worker_instance_id=%s task_id=%s attempt_number=%d error=%q",
				w.instanceID,
				task.ID,
				task.AttemptNumber,
				executionErr,
			)
		} else {
			w.logger.Printf(
				"event=task_succeeded worker_instance_id=%s task_id=%s attempt_number=%d",
				w.instanceID,
				task.ID,
				task.AttemptNumber,
			)
		}
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
