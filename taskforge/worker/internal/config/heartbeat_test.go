package config

import (
	"strings"
	"testing"
	"time"
)

func TestHeartbeatConfiguration(t *testing.T) {
	valid := Heartbeat{
		Interval:   time.Second,
		StaleAfter: 2 * time.Second,
		DeadAfter:  3 * time.Second,
		Timeout:    time.Second,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("validate valid configuration: %v", err)
	}

	tests := []struct {
		name      string
		configure func(*Heartbeat)
		message   string
	}{
		{"interval zero", func(c *Heartbeat) { c.Interval = 0 }, "HEARTBEAT_INTERVAL"},
		{"stale equals interval", func(c *Heartbeat) { c.StaleAfter = c.Interval }, "STALE_AFTER"},
		{"dead equals stale", func(c *Heartbeat) { c.DeadAfter = c.StaleAfter }, "DEAD_AFTER"},
		{"timeout zero", func(c *Heartbeat) { c.Timeout = 0 }, "HEARTBEAT_TIMEOUT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configuration := valid
			test.configure(&configuration)
			err := configuration.Validate()
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("expected %s validation error, got %v", test.message, err)
			}
		})
	}
}
