package service

import (
	"context"
	"log/slog"
	"time"

	"taskforge/scheduler/internal/config"
	"taskforge/scheduler/internal/domain"
)

type SnapshotStore interface {
	CollectSnapshot(context.Context, time.Duration, time.Duration) (domain.GlobalSnapshot, error)
}

type SnapshotMetrics interface {
	StateCollection(time.Duration, error)
	SetSnapshot(domain.GlobalSnapshot)
}

type Collector struct {
	store   SnapshotStore
	config  config.Metrics
	logger  *slog.Logger
	metrics SnapshotMetrics
}

func NewCollector(
	store SnapshotStore,
	configuration config.Metrics,
	logger *slog.Logger,
	metrics SnapshotMetrics,
) *Collector {
	return &Collector{store: store, config: configuration, logger: logger, metrics: metrics}
}

func (collector *Collector) Run(ctx context.Context) {
	collector.scan(ctx)
	ticker := time.NewTicker(collector.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collector.scan(ctx)
		}
	}
}

func (collector *Collector) scan(ctx context.Context) {
	started := time.Now()
	operationContext, cancel := context.WithTimeout(ctx, collector.config.DBTimeout)
	defer cancel()
	snapshot, err := collector.store.CollectSnapshot(
		operationContext,
		collector.config.StaleAfter,
		collector.config.DeadAfter,
	)
	collector.metrics.StateCollection(time.Since(started), err)
	if err != nil {
		if ctx.Err() == nil {
			collector.logger.Warn(
				"global metrics state collection failed",
				"event", "metrics_state_collection_failed",
				"error", err,
			)
		}
		return
	}
	collector.metrics.SetSnapshot(snapshot)
}
