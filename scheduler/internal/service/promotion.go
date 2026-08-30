package service

import (
	"context"
	"log/slog"
	"time"

	"taskforge/scheduler/internal/config"
	"taskforge/scheduler/internal/domain"
)

type RetryPromotionStore interface {
	PromoteDueRetries(context.Context, int) ([]domain.PromotedTask, error)
}

type RetryPromotionMetrics interface {
	RetryBatch(time.Duration, error)
	RetryPromoted(time.Duration)
}

type noopRetryPromotionMetrics struct{}

func (noopRetryPromotionMetrics) RetryBatch(time.Duration, error) {}
func (noopRetryPromotionMetrics) RetryPromoted(time.Duration)     {}

type RetryPromotion struct {
	store   RetryPromotionStore
	config  config.RetryPromotion
	logger  *slog.Logger
	metrics RetryPromotionMetrics
}

func NewRetryPromotion(
	store RetryPromotionStore,
	configuration config.RetryPromotion,
	logger *slog.Logger,
) *RetryPromotion {
	return NewObservedRetryPromotion(store, configuration, logger, noopRetryPromotionMetrics{})
}

func NewObservedRetryPromotion(
	store RetryPromotionStore,
	configuration config.RetryPromotion,
	logger *slog.Logger,
	metrics RetryPromotionMetrics,
) *RetryPromotion {
	return &RetryPromotion{
		store: store, config: configuration, logger: logger, metrics: metrics,
	}
}

func (promotion *RetryPromotion) Run(ctx context.Context) {
	promotion.scan(ctx)
	ticker := time.NewTicker(promotion.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			promotion.scan(ctx)
		}
	}
}

func (promotion *RetryPromotion) scan(ctx context.Context) {
	started := time.Now()
	operationContext, cancel := context.WithTimeout(ctx, promotion.config.DBTimeout)
	defer cancel()
	promoted, err := promotion.store.PromoteDueRetries(
		operationContext,
		promotion.config.BatchSize,
	)
	promotion.metrics.RetryBatch(time.Since(started), err)
	if err != nil {
		if ctx.Err() == nil {
			promotion.logger.Error(
				"retry promotion scan failed",
				"event", "task_retry_promotion_scan_failed",
				"error", err,
			)
		}
		return
	}
	if len(promoted) == 0 {
		promotion.logger.Debug("no due retries", "event", "task_retry_promotion_scan_empty")
		return
	}
	for _, task := range promoted {
		promotion.metrics.RetryPromoted(task.Lateness)
		promotion.logger.Info(
			"due retry promoted",
			"event", "task_retry_promoted",
			"task_id", task.TaskID,
			"attempt_number", task.AttemptNumber,
			"scheduled_at", task.ScheduledAt,
		)
	}
}
