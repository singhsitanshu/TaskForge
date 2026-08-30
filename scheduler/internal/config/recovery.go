package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	DefaultRecoveryInterval  = 5 * time.Second
	DefaultRecoveryBatchSize = 100
	DefaultRecoveryDBTimeout = 2 * time.Second
	MaxRecoveryBatchSize     = 10_000
)

type Recovery struct {
	Interval  time.Duration
	BatchSize int
	DBTimeout time.Duration
}

func RecoveryFromEnv() (Recovery, error) {
	interval, err := durationEnv("SCHEDULER_RECOVERY_INTERVAL", DefaultRecoveryInterval)
	if err != nil {
		return Recovery{}, err
	}
	databaseTimeout, err := durationEnv("SCHEDULER_RECOVERY_DB_TIMEOUT", DefaultRecoveryDBTimeout)
	if err != nil {
		return Recovery{}, err
	}
	batchSize, err := integerEnv("SCHEDULER_RECOVERY_BATCH_SIZE", DefaultRecoveryBatchSize)
	if err != nil {
		return Recovery{}, err
	}

	configuration := Recovery{
		Interval:  interval,
		BatchSize: batchSize,
		DBTimeout: databaseTimeout,
	}
	if err := configuration.Validate(); err != nil {
		return Recovery{}, err
	}
	return configuration, nil
}

func (configuration Recovery) Validate() error {
	if configuration.Interval <= 0 {
		return fmt.Errorf("SCHEDULER_RECOVERY_INTERVAL must be positive")
	}
	if configuration.BatchSize <= 0 || configuration.BatchSize > MaxRecoveryBatchSize {
		return fmt.Errorf(
			"SCHEDULER_RECOVERY_BATCH_SIZE must be between 1 and %d",
			MaxRecoveryBatchSize,
		)
	}
	if configuration.DBTimeout <= 0 {
		return fmt.Errorf("SCHEDULER_RECOVERY_DB_TIMEOUT must be positive")
	}
	return nil
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", key)
	}
	return duration, nil
}

func integerEnv(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	integer, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	return integer, nil
}
