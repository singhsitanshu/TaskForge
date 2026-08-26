package service

import (
	"context"
	"log/slog"
	"time"

	"taskforge/scheduler/internal/config"
	"taskforge/scheduler/internal/domain"
)

type RecoveryStore interface {
	RecoverExpired(context.Context, int) (domain.RecoveryBatch, error)
}

type RecoveryMetrics interface {
	RecoveryBatch(time.Duration, error)
	Recovered(string, time.Duration)
	TaskTotalLatency(time.Duration)
}

type noopRecoveryMetrics struct{}

func (noopRecoveryMetrics) RecoveryBatch(time.Duration, error) {}
func (noopRecoveryMetrics) Recovered(string, time.Duration)    {}
func (noopRecoveryMetrics) TaskTotalLatency(time.Duration)     {}

type Recovery struct {
	store   RecoveryStore
	config  config.Recovery
	logger  *slog.Logger
	metrics RecoveryMetrics
}

func NewRecovery(
	store RecoveryStore,
	configuration config.Recovery,
	logger *slog.Logger,
) *Recovery {
	return NewObservedRecovery(store, configuration, logger, noopRecoveryMetrics{})
}

func NewObservedRecovery(
	store RecoveryStore,
	configuration config.Recovery,
	logger *slog.Logger,
	metrics RecoveryMetrics,
) *Recovery {
	return &Recovery{
		store: store, config: configuration, logger: logger, metrics: metrics,
	}
}

func (recovery *Recovery) Run(ctx context.Context) {
	recovery.scan(ctx)
	ticker := time.NewTicker(recovery.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recovery.scan(ctx)
		}
	}
}

func (recovery *Recovery) scan(ctx context.Context) {
	started := time.Now()
	operationContext, cancel := context.WithTimeout(ctx, recovery.config.DBTimeout)
	defer cancel()

	batch, err := recovery.store.RecoverExpired(
		operationContext,
		recovery.config.BatchSize,
	)
	recovery.metrics.RecoveryBatch(time.Since(started), err)
	if err != nil {
		if ctx.Err() == nil {
			recovery.logger.Error(
				"expired task recovery scan failed",
				"event", "task_recovery_scan_failed",
				"error", err,
			)
		}
		return
	}
	if len(batch.Recovered) == 0 && len(batch.Violations) == 0 {
		recovery.logger.Debug("no expired tasks", "event", "task_recovery_scan_empty")
		return
	}

	for _, task := range batch.Recovered {
		event := "task_recovered"
		outcome := "requeued"
		if task.Action == domain.RecoveryFailed {
			event = "task_recovery_exhausted"
			outcome = "exhausted"
			recovery.metrics.TaskTotalLatency(task.TotalLatency)
		}
		recovery.metrics.Recovered(outcome, task.RecoveryLag)
		recovery.logger.Info(
			"expired task ownership recovered",
			"event", event,
			"task_id", task.TaskID,
			"old_worker_id", task.OldWorkerID,
			"attempt_number", task.AttemptNumber,
			"lease_expires_at", task.LeaseExpiresAt,
			"action", task.Action,
		)
	}
	for _, violation := range batch.Violations {
		recovery.logger.Error(
			"expired task recovery invariant violation",
			"event", "task_recovery_invariant_violation",
			"task_id", violation.TaskID,
			"old_worker_id", violation.OldWorkerID,
			"attempt_number", violation.AttemptNumber,
			"lease_expires_at", violation.LeaseExpiresAt,
			"reason", violation.Reason,
		)
	}
}
