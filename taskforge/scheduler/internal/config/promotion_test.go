package config

import (
	"strings"
	"testing"
	"time"
)

func TestRetryPromotionDefaultsAndOverrides(t *testing.T) {
	t.Setenv("SCHEDULER_RETRY_PROMOTION_INTERVAL", "")
	t.Setenv("SCHEDULER_RETRY_PROMOTION_BATCH_SIZE", "")
	t.Setenv("SCHEDULER_RETRY_PROMOTION_DB_TIMEOUT", "")
	defaults, err := RetryPromotionFromEnv()
	if err != nil {
		t.Fatalf("load promotion defaults: %v", err)
	}
	if defaults.Interval != time.Second || defaults.BatchSize != 100 || defaults.DBTimeout != 2*time.Second {
		t.Fatalf("unexpected promotion defaults: %+v", defaults)
	}

	t.Setenv("SCHEDULER_RETRY_PROMOTION_INTERVAL", "50ms")
	t.Setenv("SCHEDULER_RETRY_PROMOTION_BATCH_SIZE", "25")
	t.Setenv("SCHEDULER_RETRY_PROMOTION_DB_TIMEOUT", "100ms")
	overrides, err := RetryPromotionFromEnv()
	if err != nil {
		t.Fatalf("load promotion overrides: %v", err)
	}
	if overrides.Interval != 50*time.Millisecond || overrides.BatchSize != 25 || overrides.DBTimeout != 100*time.Millisecond {
		t.Fatalf("unexpected promotion overrides: %+v", overrides)
	}
}

func TestRetryPromotionRejectsInvalidValues(t *testing.T) {
	for _, test := range []struct{ key, value string }{
		{"SCHEDULER_RETRY_PROMOTION_INTERVAL", "0s"},
		{"SCHEDULER_RETRY_PROMOTION_BATCH_SIZE", "0"},
		{"SCHEDULER_RETRY_PROMOTION_BATCH_SIZE", "10001"},
		{"SCHEDULER_RETRY_PROMOTION_DB_TIMEOUT", "0s"},
	} {
		t.Run(test.key+test.value, func(t *testing.T) {
			t.Setenv("SCHEDULER_RETRY_PROMOTION_INTERVAL", "")
			t.Setenv("SCHEDULER_RETRY_PROMOTION_BATCH_SIZE", "")
			t.Setenv("SCHEDULER_RETRY_PROMOTION_DB_TIMEOUT", "")
			t.Setenv(test.key, test.value)
			_, err := RetryPromotionFromEnv()
			if err == nil || !strings.Contains(err.Error(), test.key) {
				t.Fatalf("expected %s error, got %v", test.key, err)
			}
		})
	}
}
