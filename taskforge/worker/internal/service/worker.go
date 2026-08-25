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
	pollInterval time.Duration
	logger       *log.Logger
}

func New(
	store Store,
	executor Executor,
	workerID string,
	pollInterval time.Duration,
	logger *log.Logger,
) *Worker {
	return &Worker{
		store:        store,
		executor:     executor,
		workerID:     workerID,
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
			"executing task id=%s type=%s attempt=%d",
			task.ID,
			task.Type,
			task.AttemptNumber,
		)
		result, executionErr := w.executor.Execute(ctx, task.Type, task.Payload)
		if err := w.store.Complete(
			ctx,
			w.workerID,
			task,
			result,
			executionErr,
		); err != nil {
			w.logger.Printf("submit task completion id=%s: %v", task.ID, err)
			if !wait(ctx, w.pollInterval) {
				return
			}
			continue
		}
		if executionErr != nil {
			w.logger.Printf("task failed id=%s: %v", task.ID, executionErr)
		} else {
			w.logger.Printf("task succeeded id=%s", task.ID)
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
