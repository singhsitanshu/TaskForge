package domain

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrLeaseLost = errors.New("task lease lost")

type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string { return e.Err.Error() }

func (e *RetryableError) Unwrap() error { return e.Err }

func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Err: err}
}

func IsRetryable(err error) bool {
	var retryable *RetryableError
	return errors.As(err, &retryable)
}

type ExecutionMetadata struct {
	TaskID        string
	AttemptNumber int16
	WorkerID      string
}

type RetryOutcome struct {
	RetryAt   time.Time
	Delay     time.Duration
	Exhausted bool
}

type ClaimedTask struct {
	ID             string
	AttemptID      string
	AttemptNumber  int16
	LeaseExpiresAt time.Time
	Type           string
	Payload        json.RawMessage
}
