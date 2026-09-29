package entity

import (
	"context"
	"encoding/json"
	"time"
)

// DLQStatus indicates the lifecycle state of a dead-lettered message.
type DLQStatus string

const (
	// DLQStatusPending means the message has been moved to the DLQ but
	// has not yet been resolved (retried or purged).
	DLQStatusPending DLQStatus = "pending"
	// DLQStatusProcessing means the message is currently being retried.
	DLQStatusProcessing DLQStatus = "processing"
	// DLQStatusLocked means the message exceeded MaxDLQRetries and is
	// permanently quarantined until manually resolved.
	DLQStatusLocked DLQStatus = "locked"
	// DLQStatusResolved means the message was successfully requeued or
	// purged.
	DLQStatusResolved DLQStatus = "resolved"
)

// DLQMessage represents a single dead-lettered event with full context
// about why it failed and how many retries have been attempted.
type DLQMessage struct {
	// ID is the Redis stream message ID of the DLQ entry.
	ID string `json:"id"`

	// Event is the original event that failed processing.
	Event *Event `json:"event"`

	// Reason explains why the message was moved to the DLQ.
	Reason string `json:"reason"`

	// RetryCount tracks how many times this message has been retried
	// since its first failure.
	RetryCount int `json:"retry_count"`

	// FailedAt is the timestamp of the most recent failure.
	FailedAt time.Time `json:"failed_at"`

	// Consumer is the name of the consumer that last touched the message.
	Consumer string `json:"consumer"`

	// Status indicates the current quarantine state (pending, locked, etc.).
	Status DLQStatus `json:"status"`
}

// DLQFilter defines query parameters for retrieving DLQ messages.
type DLQFilter struct {
	Types     []EventType
	Sources   []string
	Subjects  []string
	Status    DLQStatus
	MinRetry  int
	From      time.Time
	To        time.Time
	MaxLimit  int
	Offset    int
}

// DLQRepository defines the persistence interface for DLQ messages.
// Implementations live in the adapter layer (e.g. PostgreSQL).
type DLQRepository interface {
	// StoreDLQ persists a dead-lettered message to the durable store.
	StoreDLQ(ctx context.Context, msg *DLQMessage) error

	// QueryDLQ retrieves DLQ messages matching the filter with pagination.
	QueryDLQ(ctx context.Context, filter DLQFilter) ([]*DLQMessage, error)

	// UpdateDLQStatus updates the status and retry count of a DLQ message.
	UpdateDLQStatus(ctx context.Context, id string, status DLQStatus, retryCount int) error

	// DeleteDLQ removes DLQ records by IDs (purge).
	DeleteDLQ(ctx context.Context, ids []string) error
}

// MarshalDLQ serializes a DLQMessage for stream transport.
func MarshalDLQ(msg *DLQMessage) ([]byte, error) {
	return json.Marshal(msg)
}

// UnmarshalDLQ deserializes a DLQMessage from JSON.
func UnmarshalDLQ(data []byte) (*DLQMessage, error) {
	var msg DLQMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
