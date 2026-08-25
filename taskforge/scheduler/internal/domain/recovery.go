package domain

import "time"

const (
	AttemptAbandonReason    = "lease_expired"
	MaxAttemptsExpiredError = "max_attempts_exhausted_after_lease_expiration"
)

type RecoveryAction string

const (
	RecoveryRequeued RecoveryAction = "requeued"
	RecoveryFailed   RecoveryAction = "failed"
)

type RecoveredTask struct {
	TaskID         string
	OldWorkerID    string
	AttemptNumber  int16
	LeaseExpiresAt time.Time
	Action         RecoveryAction
}

type InvariantViolation struct {
	TaskID         string
	OldWorkerID    string
	AttemptNumber  int16
	LeaseExpiresAt time.Time
	Reason         string
}

type RecoveryBatch struct {
	Recovered  []RecoveredTask
	Violations []InvariantViolation
}
