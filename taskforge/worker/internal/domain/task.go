package domain

import "encoding/json"

type ClaimedTask struct {
	ID            string
	AttemptID     string
	AttemptNumber int16
	Type          string
	Payload       json.RawMessage
}
