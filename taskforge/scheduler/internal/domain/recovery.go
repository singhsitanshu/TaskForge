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
	RecoveryLag    time.Duration
	Action         RecoveryAction
	TotalLatency   time.Duration
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

type PromotedTask struct {
	TaskID        string
	AttemptNumber int16
	ScheduledAt   time.Time
	Lateness      time.Duration
}

type GlobalSnapshot struct {
	TaskCounts           map[string]int64
	WorkerCounts         map[string]int64
	RunningAttempts      int64
	EligibleTasks        int64
	ExpiredRunningLeases int64
}
