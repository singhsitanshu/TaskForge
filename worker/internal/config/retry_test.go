package config

import (
	"math"
	"testing"
	"time"
)

func TestRetryExponentialBackoffAndCap(t *testing.T) {
	configuration := Retry{BaseDelay: 2 * time.Second, MaxDelay: 30 * time.Second}
	expected := []time.Duration{
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}
	for retryIndex, expectedDelay := range expected {
		if delay := configuration.Delay(retryIndex, 0.5); delay != expectedDelay {
			t.Fatalf("retry index %d delay=%s expected=%s", retryIndex, delay, expectedDelay)
		}
	}
	if delay := configuration.Delay(math.MaxInt, 0.5); delay != 30*time.Second {
		t.Fatalf("large retry index overflowed: %s", delay)
	}
}

func TestRetryJitterBoundsAndCap(t *testing.T) {
	configuration := Retry{
		BaseDelay: 10 * time.Second,
		MaxDelay:  15 * time.Second,
		Jitter:    0.2,
	}
	if delay := configuration.Delay(0, 0); delay != 8*time.Second {
		t.Fatalf("lower bound=%s", delay)
	}
	if delay := configuration.Delay(0, 1); delay != 12*time.Second {
		t.Fatalf("upper bound=%s", delay)
	}
	if delay := configuration.Delay(1, 1); delay != 15*time.Second {
		t.Fatalf("cap not respected: %s", delay)
	}
	if delay := configuration.Delay(0, -10); delay < 0 {
		t.Fatalf("negative random input produced %s", delay)
	}
}

func TestRetryValidation(t *testing.T) {
	invalid := []Retry{
		{},
		{BaseDelay: time.Second, MaxDelay: 500 * time.Millisecond},
		{BaseDelay: time.Second, MaxDelay: time.Second, Jitter: -0.1},
		{BaseDelay: time.Second, MaxDelay: time.Second, Jitter: 1},
	}
	for _, configuration := range invalid {
		if err := configuration.Validate(); err == nil {
			t.Fatalf("expected invalid configuration: %+v", configuration)
		}
	}
}
