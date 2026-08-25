package service

import (
	"context"
	"log"
	"time"
)

type HeartbeatStore interface {
	Heartbeat(context.Context, string, string) error
}

type Heartbeater struct {
	store      HeartbeatStore
	workerID   string
	instanceID string
	interval   time.Duration
	timeout    time.Duration
	logger     *log.Logger
}

func NewHeartbeater(
	store HeartbeatStore,
	workerID string,
	instanceID string,
	interval time.Duration,
	timeout time.Duration,
	logger *log.Logger,
) *Heartbeater {
	return &Heartbeater{
		store:      store,
		workerID:   workerID,
		instanceID: instanceID,
		interval:   interval,
		timeout:    timeout,
		logger:     logger,
	}
}

func (h *Heartbeater) Run(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			operationContext, cancel := context.WithTimeout(ctx, h.timeout)
			err := h.store.Heartbeat(operationContext, h.workerID, h.instanceID)
			cancel()
			if err != nil && ctx.Err() == nil {
				h.logger.Printf(
					"event=heartbeat_failed worker_instance_id=%s worker_id=%s error=%q",
					h.instanceID,
					h.workerID,
					err,
				)
			}
		}
	}
}
