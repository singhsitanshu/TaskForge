package config

import (
	"strings"
	"testing"
	"time"
)

func TestRecoveryDefaults(t *testing.T) {
	t.Setenv("SCHEDULER_RECOVERY_INTERVAL", "")
	t.Setenv("SCHEDULER_RECOVERY_BATCH_SIZE", "")
	t.Setenv("SCHEDULER_RECOVERY_DB_TIMEOUT", "")

	configuration, err := RecoveryFromEnv()
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}
	if configuration.Interval != 5*time.Second ||
		configuration.BatchSize != 100 ||
		configuration.DBTimeout != 2*time.Second {
		t.Fatalf("unexpected defaults: %+v", configuration)
	}
}

func TestRecoveryEnvironmentOverrides(t *testing.T) {
	t.Setenv("SCHEDULER_RECOVERY_INTERVAL", "250ms")
	t.Setenv("SCHEDULER_RECOVERY_BATCH_SIZE", "25")
	t.Setenv("SCHEDULER_RECOVERY_DB_TIMEOUT", "75ms")

	configuration, err := RecoveryFromEnv()
	if err != nil {
		t.Fatalf("load overrides: %v", err)
	}
	if configuration.Interval != 250*time.Millisecond ||
		configuration.BatchSize != 25 ||
		configuration.DBTimeout != 75*time.Millisecond {
		t.Fatalf("unexpected overrides: %+v", configuration)
	}
}

func TestRecoveryRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "zero interval", key: "SCHEDULER_RECOVERY_INTERVAL", value: "0s"},
		{name: "invalid interval", key: "SCHEDULER_RECOVERY_INTERVAL", value: "later"},
		{name: "zero batch", key: "SCHEDULER_RECOVERY_BATCH_SIZE", value: "0"},
		{name: "oversized batch", key: "SCHEDULER_RECOVERY_BATCH_SIZE", value: "10001"},
		{name: "invalid batch", key: "SCHEDULER_RECOVERY_BATCH_SIZE", value: "many"},
		{name: "zero timeout", key: "SCHEDULER_RECOVERY_DB_TIMEOUT", value: "0s"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("SCHEDULER_RECOVERY_INTERVAL", "")
			t.Setenv("SCHEDULER_RECOVERY_BATCH_SIZE", "")
			t.Setenv("SCHEDULER_RECOVERY_DB_TIMEOUT", "")
			t.Setenv(test.key, test.value)
			_, err := RecoveryFromEnv()
			if err == nil || !strings.Contains(err.Error(), test.key) {
				t.Fatalf("expected %s validation error, got %v", test.key, err)
			}
		})
	}
}
