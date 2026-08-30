package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"taskforge/scheduler/internal/config"
	"taskforge/scheduler/internal/domain"
)

type snapshotStoreStub struct {
	snapshot domain.GlobalSnapshot
	err      error
}

func (store *snapshotStoreStub) CollectSnapshot(
	context.Context,
	time.Duration,
	time.Duration,
) (domain.GlobalSnapshot, error) {
	return store.snapshot, store.err
}

type snapshotMetricsSpy struct {
	collections int
	errors      int
	snapshots   []domain.GlobalSnapshot
}

func (metrics *snapshotMetricsSpy) StateCollection(_ time.Duration, err error) {
	metrics.collections++
	if err != nil {
		metrics.errors++
	}
}

func (metrics *snapshotMetricsSpy) SetSnapshot(snapshot domain.GlobalSnapshot) {
	metrics.snapshots = append(metrics.snapshots, snapshot)
}

func TestCollectorReplacesSnapshotAndRetainsItOnFailure(t *testing.T) {
	store := &snapshotStoreStub{snapshot: domain.GlobalSnapshot{
		TaskCounts: map[string]int64{"QUEUED": 10},
	}}
	metrics := &snapshotMetricsSpy{}
	collector := NewCollector(store, config.Metrics{
		Interval: time.Hour, DBTimeout: time.Second,
		StaleAfter: 15 * time.Second, DeadAfter: 30 * time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics)

	collector.scan(context.Background())
	store.err = errors.New("database unavailable")
	collector.scan(context.Background())

	if metrics.collections != 2 || metrics.errors != 1 || len(metrics.snapshots) != 1 {
		t.Fatalf("collections=%d errors=%d snapshots=%d", metrics.collections, metrics.errors, len(metrics.snapshots))
	}
	if metrics.snapshots[0].TaskCounts["QUEUED"] != 10 {
		t.Fatalf("unexpected snapshot: %+v", metrics.snapshots[0])
	}
}
