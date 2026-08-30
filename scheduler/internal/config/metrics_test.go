package config

import (
	"testing"
	"time"
)

func TestMetricsFromEnv(t *testing.T) {
	t.Setenv("SCHEDULER_METRICS_INTERVAL", "3s")
	t.Setenv("SCHEDULER_METRICS_DB_TIMEOUT", "750ms")
	t.Setenv("WORKER_STALE_AFTER", "10s")
	t.Setenv("WORKER_DEAD_AFTER", "20s")
	configuration, err := MetricsFromEnv()
	if err != nil {
		t.Fatalf("metrics config: %v", err)
	}
	if configuration.Interval != 3*time.Second ||
		configuration.DBTimeout != 750*time.Millisecond ||
		configuration.StaleAfter != 10*time.Second ||
		configuration.DeadAfter != 20*time.Second {
		t.Fatalf("unexpected configuration: %+v", configuration)
	}
}

func TestMetricsRejectsOverlappingLivenessThresholds(t *testing.T) {
	t.Setenv("WORKER_STALE_AFTER", "30s")
	t.Setenv("WORKER_DEAD_AFTER", "30s")
	if _, err := MetricsFromEnv(); err == nil {
		t.Fatal("expected invalid liveness thresholds")
	}
}
