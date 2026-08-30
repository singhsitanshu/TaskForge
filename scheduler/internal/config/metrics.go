package config

import (
	"fmt"
	"time"
)

const (
	DefaultMetricsInterval  = 5 * time.Second
	DefaultMetricsDBTimeout = 2 * time.Second
	DefaultWorkerStaleAfter = 15 * time.Second
	DefaultWorkerDeadAfter  = 30 * time.Second
)

type Metrics struct {
	Interval   time.Duration
	DBTimeout  time.Duration
	StaleAfter time.Duration
	DeadAfter  time.Duration
}

func MetricsFromEnv() (Metrics, error) {
	interval, err := durationEnv("SCHEDULER_METRICS_INTERVAL", DefaultMetricsInterval)
	if err != nil {
		return Metrics{}, err
	}
	databaseTimeout, err := durationEnv("SCHEDULER_METRICS_DB_TIMEOUT", DefaultMetricsDBTimeout)
	if err != nil {
		return Metrics{}, err
	}
	staleAfter, err := durationEnv("WORKER_STALE_AFTER", DefaultWorkerStaleAfter)
	if err != nil {
		return Metrics{}, err
	}
	deadAfter, err := durationEnv("WORKER_DEAD_AFTER", DefaultWorkerDeadAfter)
	if err != nil {
		return Metrics{}, err
	}
	configuration := Metrics{
		Interval: interval, DBTimeout: databaseTimeout,
		StaleAfter: staleAfter, DeadAfter: deadAfter,
	}
	if err := configuration.Validate(); err != nil {
		return Metrics{}, err
	}
	return configuration, nil
}

func (configuration Metrics) Validate() error {
	if configuration.Interval <= 0 {
		return fmt.Errorf("SCHEDULER_METRICS_INTERVAL must be positive")
	}
	if configuration.DBTimeout <= 0 {
		return fmt.Errorf("SCHEDULER_METRICS_DB_TIMEOUT must be positive")
	}
	if configuration.StaleAfter <= 0 {
		return fmt.Errorf("WORKER_STALE_AFTER must be positive")
	}
	if configuration.DeadAfter <= configuration.StaleAfter {
		return fmt.Errorf("WORKER_DEAD_AFTER must be greater than WORKER_STALE_AFTER")
	}
	return nil
}
