package config

import (
	"errors"
	"time"
)

const (
	DefaultHeartbeatInterval = 5 * time.Second
	DefaultStaleAfter        = 15 * time.Second
	DefaultDeadAfter         = 30 * time.Second
	DefaultHeartbeatTimeout  = 2 * time.Second
)

type Heartbeat struct {
	Interval   time.Duration
	StaleAfter time.Duration
	DeadAfter  time.Duration
	Timeout    time.Duration
}

func (c Heartbeat) Validate() error {
	if c.Interval <= 0 {
		return errors.New("WORKER_HEARTBEAT_INTERVAL must be positive")
	}
	if c.StaleAfter <= c.Interval {
		return errors.New("WORKER_STALE_AFTER must be greater than WORKER_HEARTBEAT_INTERVAL")
	}
	if c.DeadAfter <= c.StaleAfter {
		return errors.New("WORKER_DEAD_AFTER must be greater than WORKER_STALE_AFTER")
	}
	if c.Timeout <= 0 {
		return errors.New("WORKER_HEARTBEAT_TIMEOUT must be positive")
	}
	return nil
}
