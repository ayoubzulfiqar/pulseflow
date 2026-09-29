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
