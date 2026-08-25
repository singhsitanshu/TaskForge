package domain

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrLeaseLost = errors.New("task lease lost")

type ClaimedTask struct {
	ID             string
	AttemptID      string
	AttemptNumber  int16
	LeaseExpiresAt time.Time
	Type           string
	Payload        json.RawMessage
}
