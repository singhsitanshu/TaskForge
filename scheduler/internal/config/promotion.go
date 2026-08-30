package config

import (
	"fmt"
	"time"
)

const (
	DefaultRetryPromotionInterval  = time.Second
	DefaultRetryPromotionBatchSize = 100
	DefaultRetryPromotionDBTimeout = 2 * time.Second
)

type RetryPromotion struct {
	Interval  time.Duration
	BatchSize int
	DBTimeout time.Duration
}

func RetryPromotionFromEnv() (RetryPromotion, error) {
	interval, err := durationEnv(
		"SCHEDULER_RETRY_PROMOTION_INTERVAL",
		DefaultRetryPromotionInterval,
	)
	if err != nil {
		return RetryPromotion{}, err
	}
	databaseTimeout, err := durationEnv(
		"SCHEDULER_RETRY_PROMOTION_DB_TIMEOUT",
		DefaultRetryPromotionDBTimeout,
	)
	if err != nil {
		return RetryPromotion{}, err
	}
	batchSize, err := integerEnv(
		"SCHEDULER_RETRY_PROMOTION_BATCH_SIZE",
		DefaultRetryPromotionBatchSize,
	)
	if err != nil {
		return RetryPromotion{}, err
	}
	configuration := RetryPromotion{
		Interval: interval, BatchSize: batchSize, DBTimeout: databaseTimeout,
	}
	if err := configuration.Validate(); err != nil {
		return RetryPromotion{}, err
	}
	return configuration, nil
}

func (configuration RetryPromotion) Validate() error {
	if configuration.Interval <= 0 {
		return fmt.Errorf("SCHEDULER_RETRY_PROMOTION_INTERVAL must be positive")
	}
	if configuration.BatchSize <= 0 || configuration.BatchSize > MaxRecoveryBatchSize {
		return fmt.Errorf(
			"SCHEDULER_RETRY_PROMOTION_BATCH_SIZE must be between 1 and %d",
			MaxRecoveryBatchSize,
		)
	}
	if configuration.DBTimeout <= 0 {
		return fmt.Errorf("SCHEDULER_RETRY_PROMOTION_DB_TIMEOUT must be positive")
	}
	return nil
}
