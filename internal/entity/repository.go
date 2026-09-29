package entity

import (
	"context"
)

// EventRepository defines the persistence interface for events.
// Implementations live in the adapter layer (e.g. PostgreSQL).
type EventRepository interface {
	// Store persists an event to the durable store.
	Store(ctx context.Context, event *Event) error

	// GetByID retrieves a single event by its ULID.
	// Returns ErrNotFound if the event does not exist.
	GetByID(ctx context.Context, id EventID) (*Event, error)

	// Query retrieves events matching the filter, respecting MaxLimit
	// and Offset for pagination.
	Query(ctx context.Context, filter EventFilter) ([]*Event, error)

	// QueryDLQ retrieves DLQ messages matching the filter with pagination.
	// Returns DLQMessage pointers (including the embedded Event).
	QueryDLQ(ctx context.Context, filter DLQFilter) ([]*DLQMessage, error)

	// GetDLQByID retrieves a single DLQ message by its ID.
	// Returns ErrDLQMessageNotFound if it does not exist.
	GetDLQByID(ctx context.Context, id string) (*DLQMessage, error)

	// StoreDLQ persists a dead-lettered message to the durable store.
	StoreDLQ(ctx context.Context, msg *DLQMessage) error

	// UpdateDLQStatus updates the status and retry count of a DLQ record.
	UpdateDLQStatus(ctx context.Context, id string, status DLQStatus, retryCount int) error

	// DeleteDLQ removes DLQ records by IDs.
	DeleteDLQ(ctx context.Context, ids []string) (int, error)
}

// EventStream defines the streaming interface for publishing and consuming
// events via Redis Streams (or any compatible streaming backend).
type EventStream interface {
	// Publish writes an event to the configured stream.
	Publish(ctx context.Context, event *Event) error

	// EnsureGroup creates the consumer group if it does not already exist.
	// Idempotent — safe to call on every startup.
	EnsureGroup(ctx context.Context) error

	// Consume starts a consumer that reads from the stream via the
	// consumer group. The handler is called for each batch of messages.
	// The consumer name is unique per worker instance.
	// Blocks until ctx is cancelled or a fatal error occurs.
	Consume(ctx context.Context, consumerName string, batchSize int, handler StreamHandler) error

	// Ack acknowledges one or more message IDs as successfully processed.
	Ack(ctx context.Context, ids []string) error

	// ClaimStaleMessages rescues pending messages idle longer than minIdle
	// from other consumers, providing failover when a consumer dies.
	ClaimStaleMessages(ctx context.Context, consumerName string, minIdle string, batchSize int) ([]StreamMessage, error)

	// DeadLetterQueue moves failed messages to the DLQ stream for
	// later inspection and retry.
	DeadLetterQueue(ctx context.Context, messages []StreamMessage, reason string) error

	// ListDLQ reads messages from the dead-letter queue stream.
	// Returns them in stream order; limit=0 means no artificial cap.
	ListDLQ(ctx context.Context, limit, offset int) ([]StreamMessage, error)

	// RequeueDLQ moves messages from the DLQ stream back to the main
	// stream so they can be reprocessed. The caller has already decided
	// which IDs to retry (e.g. based on DB-persisted DLQ records).
	RequeueDLQ(ctx context.Context, ids []string) error

	// PurgeDLQ removes all messages from the dead-letter queue stream.
	// Returns the number of entries deleted.
	PurgeDLQ(ctx context.Context) (int, error)
}

// StreamMessage represents a single message read from a Redis Stream.
type StreamMessage struct {
	ID      string
	Stream  string
	Consumer string
	Body    map[string]interface{}
}

// StreamHandler is called by Consume for each batch of messages.
// The handler must ack or nack each message by returning nil or an error
// respectively. A non-nil error triggers DLQ routing for that message.
type StreamHandler func(ctx context.Context, msg StreamMessage) error

// NotFoundError is returned by EventRepository.GetByID when the event
// does not exist in the store.
type NotFoundError struct {
	ID string
}

func (e *NotFoundError) Error() string {
	return "entity: event not found: " + e.ID
}

// ErrNotFound is a package-level sentinel for convenience.
var ErrNotFound = new(NotFoundError)

// CircuitBreakerState exposes the runtime state of a circuit breaker
// wrapping an EventStream. This allows admin endpoints to query and
// reset the breaker for observability and operational control.
type CircuitBreakerState interface {
	// Name returns the circuit breaker identifier.
	Name() string
	// State returns the current breaker state ("closed", "open", "half-open").
	State() string
	// Failing returns the current consecutive failure count.
	Failing() int
	// TotalCalls returns the total number of calls since last reset.
	TotalCalls() int
	// Reset manually resets the circuit breaker to closed.
	Reset()
}
