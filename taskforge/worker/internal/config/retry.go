package config

import (
	"fmt"
	"math"
	"time"
)

const (
	DefaultRetryBaseDelay = 2 * time.Second
	DefaultRetryMaxDelay  = 5 * time.Minute
	DefaultRetryJitter    = 0.2
)

type Retry struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Jitter    float64
}

func (configuration Retry) Validate() error {
	if configuration.BaseDelay <= 0 {
		return fmt.Errorf("TASK_RETRY_BASE_DELAY must be positive")
	}
	if configuration.MaxDelay <= 0 {
		return fmt.Errorf("TASK_RETRY_MAX_DELAY must be positive")
	}
	if configuration.MaxDelay < configuration.BaseDelay {
		return fmt.Errorf("TASK_RETRY_MAX_DELAY must be at least TASK_RETRY_BASE_DELAY")
	}
	if configuration.Jitter < 0 || configuration.Jitter >= 1 {
		return fmt.Errorf("TASK_RETRY_JITTER must be between 0 inclusive and 1 exclusive")
	}
	return nil
}

// Delay returns the delay for a zero-based retry index. randomUnit must be in
// [0,1]; values outside that range are clamped to keep the result bounded.
func (configuration Retry) Delay(retryIndex int, randomUnit float64) time.Duration {
	if retryIndex < 0 {
		retryIndex = 0
	}
	delay := configuration.BaseDelay
	for range retryIndex {
		if delay >= configuration.MaxDelay || delay > configuration.MaxDelay/2 {
			delay = configuration.MaxDelay
			break
		}
		delay *= 2
	}
	if delay > configuration.MaxDelay {
		delay = configuration.MaxDelay
	}
	if configuration.Jitter == 0 {
		return delay
	}
	randomUnit = math.Max(0, math.Min(1, randomUnit))
	multiplier := 1 - configuration.Jitter + 2*configuration.Jitter*randomUnit
	jittered := time.Duration(float64(delay) * multiplier)
	if jittered < 0 {
		return 0
	}
	if jittered > configuration.MaxDelay {
		return configuration.MaxDelay
	}
	return jittered
}
