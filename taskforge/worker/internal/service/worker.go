package service

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"time"

	"taskforge/worker/internal/domain"
)

type Store interface {
	ClaimNext(context.Context, string, ...time.Duration) (*domain.ClaimedTask, error)
	RenewLease(
		context.Context,
		string,
		string,
		int16,
		time.Duration,
	) (time.Time, error)
	Complete(
		context.Context,
		string,
		*domain.ClaimedTask,
		map[string]any,
		error,
	) error
	RetryableFail(
		context.Context,
		string,
		*domain.ClaimedTask,
		error,
		time.Duration,
	) (domain.RetryOutcome, error)
}

type Executor interface {
	Execute(
		context.Context,
		string,
		json.RawMessage,
		domain.ExecutionMetadata,
	) (map[string]any, error)
}

type RetryDelay func(retryIndex int) time.Duration

type WorkerMetrics interface {
	TaskClaimed(time.Duration, time.Duration)
	Execution(time.Duration)
	TaskCompleted(string)
	Attempt(string)
	LeaseRenewal(string, time.Duration)
	LeaseLost()
	RetryScheduled(time.Duration)
	TaskTotalLatency(time.Duration)
}

type noopWorkerMetrics struct{}

func (noopWorkerMetrics) TaskClaimed(time.Duration, time.Duration) {}
func (noopWorkerMetrics) Execution(time.Duration)                  {}
func (noopWorkerMetrics) TaskCompleted(string)                     {}
func (noopWorkerMetrics) Attempt(string)                           {}
func (noopWorkerMetrics) LeaseRenewal(string, time.Duration)       {}
func (noopWorkerMetrics) LeaseLost()                               {}
func (noopWorkerMetrics) RetryScheduled(time.Duration)             {}
func (noopWorkerMetrics) TaskTotalLatency(time.Duration)           {}

type Worker struct {
	store              Store
	executor           Executor
	workerID           string
	instanceID         string
	pollInterval       time.Duration
	leaseDuration      time.Duration
	leaseRenewInterval time.Duration
	leaseRenewTimeout  time.Duration
	retryDelay         RetryDelay
	metrics            WorkerMetrics
	logger             *log.Logger
}

func New(
	store Store,
	executor Executor,
	workerID string,
	instanceID string,
	pollInterval time.Duration,
	leaseDuration time.Duration,
	leaseRenewInterval time.Duration,
	leaseRenewTimeout time.Duration,
	logger *log.Logger,
	retryDelays ...RetryDelay,
) *Worker {
	return NewObserved(
		store,
		executor,
		workerID,
		instanceID,
		pollInterval,
		leaseDuration,
		leaseRenewInterval,
		leaseRenewTimeout,
		logger,
		noopWorkerMetrics{},
		retryDelays...,
	)
}

func NewObserved(
	store Store,
	executor Executor,
	workerID string,
	instanceID string,
	pollInterval time.Duration,
	leaseDuration time.Duration,
	leaseRenewInterval time.Duration,
	leaseRenewTimeout time.Duration,
	logger *log.Logger,
	metrics WorkerMetrics,
	retryDelays ...RetryDelay,
) *Worker {
	retryDelay := RetryDelay(func(_ int) time.Duration { return 2 * time.Second })
	if len(retryDelays) > 0 && retryDelays[0] != nil {
		retryDelay = retryDelays[0]
	}
	return &Worker{
		store:              store,
		executor:           executor,
		workerID:           workerID,
		instanceID:         instanceID,
		pollInterval:       pollInterval,
		leaseDuration:      leaseDuration,
		leaseRenewInterval: leaseRenewInterval,
		leaseRenewTimeout:  leaseRenewTimeout,
		retryDelay:         retryDelay,
		metrics:            metrics,
		logger:             logger,
	}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		claimStarted := time.Now()
		task, err := w.store.ClaimNext(ctx, w.workerID, w.leaseDuration)
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
		w.metrics.TaskClaimed(
			time.Since(claimStarted),
			task.StartedAt.Sub(task.QueuedAt),
		)

		w.logger.Printf(
			"event=task_claimed worker_instance_id=%s task_id=%s attempt_number=%d task_type=%s",
			w.instanceID,
			task.ID,
			task.AttemptNumber,
			task.Type,
		)
		w.executeClaimedTask(ctx, task)
	}
}

func (w *Worker) executeClaimedTask(ctx context.Context, task *domain.ClaimedTask) {
	w.logger.Printf(
		"event=task_lease_started worker_instance_id=%s worker_id=%s task_id=%s attempt_number=%d lease_expires_at=%s",
		w.instanceID,
		w.workerID,
		task.ID,
		task.AttemptNumber,
		task.LeaseExpiresAt.Format(time.RFC3339Nano),
	)

	handlerContext, cancelHandler := context.WithCancel(ctx)
	defer cancelHandler()
	renewalContext, stopRenewal := context.WithCancel(ctx)
	renewalResult := make(chan error, 1)
	go func() {
		renewalResult <- w.renewTaskLease(
			renewalContext,
			cancelHandler,
			task,
		)
	}()

	executionStarted := time.Now()
	result, executionErr := w.executor.Execute(
		handlerContext,
		task.Type,
		task.Payload,
		domain.ExecutionMetadata{
			TaskID:        task.ID,
			AttemptNumber: task.AttemptNumber,
			WorkerID:      w.workerID,
		},
	)
	w.metrics.Execution(time.Since(executionStarted))
	stopRenewal()
	renewalErr := <-renewalResult
	if errors.Is(renewalErr, domain.ErrLeaseLost) {
		return
	}
	if domain.IsRetryable(executionErr) {
		retryDelay := w.retryDelay(int(task.AttemptNumber) - 1)
		outcome, err := w.store.RetryableFail(
			ctx,
			w.workerID,
			task,
			executionErr,
			retryDelay,
		)
		if err != nil {
			if errors.Is(err, domain.ErrLeaseLost) {
				w.logger.Printf(
					"event=task_retry_rejected_stale_owner worker_instance_id=%s worker_id=%s task_id=%s attempt_number=%d error=%q",
					w.instanceID,
					w.workerID,
					task.ID,
					task.AttemptNumber,
					err,
				)
				return
			}
			w.logger.Printf(
				"event=task_retry_schedule_failed worker_instance_id=%s task_id=%s attempt_number=%d error=%q",
				w.instanceID,
				task.ID,
				task.AttemptNumber,
				err,
			)
			return
		}
		if outcome.Exhausted {
			w.metrics.TaskCompleted("retryable_failure")
			w.metrics.Attempt("failed")
			w.metrics.TaskTotalLatency(outcome.CompletedAt.Sub(task.CreatedAt))
			w.logger.Printf(
				"event=task_retry_exhausted worker_instance_id=%s task_id=%s attempt_number=%d error=%q",
				w.instanceID,
				task.ID,
				task.AttemptNumber,
				executionErr,
			)
			return
		}
		w.metrics.TaskCompleted("retryable_failure")
		w.metrics.Attempt("failed")
		w.metrics.RetryScheduled(outcome.Delay)
		w.logger.Printf(
			"event=task_retry_scheduled worker_instance_id=%s task_id=%s attempt_number=%d retry_at=%s retry_delay=%s error=%q",
			w.instanceID,
			task.ID,
			task.AttemptNumber,
			outcome.RetryAt.Format(time.RFC3339Nano),
			outcome.Delay,
			executionErr,
		)
		return
	}

	if err := w.store.Complete(
		ctx,
		w.workerID,
		task,
		result,
		executionErr,
	); err != nil {
		if errors.Is(err, domain.ErrLeaseLost) {
			w.logger.Printf(
				"event=task_completion_rejected_stale_owner worker_instance_id=%s worker_id=%s task_id=%s attempt_number=%d error=%q",
				w.instanceID,
				w.workerID,
				task.ID,
				task.AttemptNumber,
				err,
			)
			return
		}
		w.logger.Printf(
			"event=task_completion_failed worker_instance_id=%s task_id=%s attempt_number=%d error=%q",
			w.instanceID,
			task.ID,
			task.AttemptNumber,
			err,
		)
		return
	}
	if executionErr != nil {
		w.metrics.TaskCompleted("terminal_failure")
		w.metrics.Attempt("failed")
		w.metrics.TaskTotalLatency(task.CompletedAt.Sub(task.CreatedAt))
		w.logger.Printf(
			"event=task_failed worker_instance_id=%s task_id=%s attempt_number=%d error=%q",
			w.instanceID,
			task.ID,
			task.AttemptNumber,
			executionErr,
		)
	} else {
		w.metrics.TaskCompleted("success")
		w.metrics.Attempt("succeeded")
		w.metrics.TaskTotalLatency(task.CompletedAt.Sub(task.CreatedAt))
		w.logger.Printf(
			"event=task_succeeded worker_instance_id=%s task_id=%s attempt_number=%d",
			w.instanceID,
			task.ID,
			task.AttemptNumber,
		)
	}
}

func (w *Worker) renewTaskLease(
	ctx context.Context,
	cancelHandler context.CancelFunc,
	task *domain.ClaimedTask,
) error {
	ticker := time.NewTicker(w.leaseRenewInterval)
	defer ticker.Stop()
	confirmationDeadline := time.NewTimer(w.leaseDuration)
	defer confirmationDeadline.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-confirmationDeadline.C:
			cancelHandler()
			w.metrics.LeaseLost()
			w.logLeaseLost(task, "lease renewal confirmation deadline exceeded")
			return domain.ErrLeaseLost
		case <-ticker.C:
			startedAt := time.Now()
			operationContext, cancel := context.WithTimeout(ctx, w.leaseRenewTimeout)
			leaseExpiresAt, err := w.store.RenewLease(
				operationContext,
				task.ID,
				w.workerID,
				task.AttemptNumber,
				w.leaseDuration,
			)
			cancel()
			if err == nil {
				w.metrics.LeaseRenewal("success", time.Since(startedAt))
				task.LeaseExpiresAt = leaseExpiresAt
				slog.Debug(
					"task lease renewed",
					"event", "task_lease_renewed",
					"worker_instance_id", w.instanceID,
					"worker_id", w.workerID,
					"task_id", task.ID,
					"attempt_number", task.AttemptNumber,
					"lease_expires_at", leaseExpiresAt.Format(time.RFC3339Nano),
				)
				resetTimer(confirmationDeadline, w.leaseDuration)
				continue
			}
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, domain.ErrLeaseLost) {
				w.metrics.LeaseRenewal("lost", time.Since(startedAt))
				cancelHandler()
				w.logLeaseLost(task, err.Error())
				return domain.ErrLeaseLost
			}
			w.metrics.LeaseRenewal("error", time.Since(startedAt))
			if ctx.Err() == nil {
				w.logger.Printf(
					"event=task_lease_renew_failed worker_instance_id=%s worker_id=%s task_id=%s attempt_number=%d error=%q",
					w.instanceID,
					w.workerID,
					task.ID,
					task.AttemptNumber,
					err,
				)
			}
		}
	}
}

func (w *Worker) logLeaseLost(task *domain.ClaimedTask, message string) {
	w.logger.Printf(
		"event=task_lease_lost worker_instance_id=%s worker_id=%s task_id=%s attempt_number=%d error=%q",
		w.instanceID,
		w.workerID,
		task.ID,
		task.AttemptNumber,
		message,
	)
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
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
