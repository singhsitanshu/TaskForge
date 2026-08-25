package config

import (
	"strings"
	"testing"
	"time"
)

func TestLeaseConfiguration(t *testing.T) {
	valid := Lease{Duration: time.Second, RenewInterval: 400 * time.Millisecond, RenewTimeout: 100 * time.Millisecond}
	if err := valid.Validate(); err != nil {
		t.Fatalf("validate lease configuration: %v", err)
	}

	tests := []struct {
		name    string
		change  func(*Lease)
		message string
	}{
		{"duration", func(c *Lease) { c.Duration = 0 }, "LEASE_DURATION"},
		{"interval zero", func(c *Lease) { c.RenewInterval = 0 }, "RENEW_INTERVAL"},
		{"interval margin", func(c *Lease) { c.RenewInterval = 600 * time.Millisecond }, "half"},
		{"timeout zero", func(c *Lease) { c.RenewTimeout = 0 }, "RENEW_TIMEOUT"},
		{"timeout duration", func(c *Lease) { c.RenewTimeout = c.Duration }, "less than"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configuration := valid
			test.change(&configuration)
			err := configuration.Validate()
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("expected %q error, got %v", test.message, err)
			}
		})
	}
}
