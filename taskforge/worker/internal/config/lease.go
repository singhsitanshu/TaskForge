package config

import (
	"errors"
	"time"
)

const (
	DefaultTaskLeaseDuration      = 30 * time.Second
	DefaultTaskLeaseRenewInterval = 10 * time.Second
	DefaultTaskLeaseRenewTimeout  = 2 * time.Second
)

type Lease struct {
	Duration      time.Duration
	RenewInterval time.Duration
	RenewTimeout  time.Duration
}

func (c Lease) Validate() error {
	if c.Duration <= 0 {
		return errors.New("WORKER_TASK_LEASE_DURATION must be positive")
	}
	if c.RenewInterval <= 0 {
		return errors.New("WORKER_TASK_LEASE_RENEW_INTERVAL must be positive")
	}
	if c.RenewInterval > c.Duration/2 {
		return errors.New("WORKER_TASK_LEASE_RENEW_INTERVAL must not exceed half of WORKER_TASK_LEASE_DURATION")
	}
	if c.RenewTimeout <= 0 {
		return errors.New("WORKER_TASK_LEASE_RENEW_TIMEOUT must be positive")
	}
	if c.RenewTimeout >= c.Duration {
		return errors.New("WORKER_TASK_LEASE_RENEW_TIMEOUT must be less than WORKER_TASK_LEASE_DURATION")
	}
	return nil
}
