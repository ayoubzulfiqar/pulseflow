package entity

import "context"

// DestinationRepository defines the persistence interface for webhook
// destination configuration. Implemented by the PostgreSQL adapter.
type DestinationRepository interface {
	// ListActive returns all active destinations whose EventPattern matches
	// the given event type. Returns empty slice if no destinations match.
	ListActive(ctx context.Context, eventType EventType, source string) ([]*Destination, error)

	// GetDestination returns a single destination by ID.
	GetDestination(ctx context.Context, id string) (*Destination, error)

	// DisableDestination sets status = 'disabled' for the given destination.
	// Called when an endpoint returns HTTP 410 Gone.
	DisableDestination(ctx context.Context, id string) error
}

// WebhookDeliverer is the port (interface) that the usecase layer
// uses to send webhook notifications to individual destinations.
// The adapter implementation handles dual-secret signing, concurrency
// limiting, and HTTP 410 auto-disable.
type WebhookDeliverer interface {
	// Deliver sends the event to the specified destination.
	// Implementations handle signing, retries, concurrency, and 410 handling.
	Deliver(ctx context.Context, dest *Destination, event *Event)
}

// CELFilterer is the port (interface) for evaluating CEL expressions
// against events. The adapter implementation uses cel-go.
type CELFilterer interface {
	// ShouldDeliver evaluates the given CEL expression against the event.
	// Returns true if the event matches the filter and should be delivered.
	// If the expression is empty, returns true (no filter = allow all).
	ShouldDeliver(ctx context.Context, expr string, event *Event) (bool, error)
}

// ConcurrencyLimiter is the port (interface) for limiting concurrent
// outbound webhook requests per destination URL.
type ConcurrencyLimiter interface {
	// Acquire attempts to acquire a concurrency slot for the given key
	// up to the specified limit. Returns true if acquired (caller must
	// call Release when done), false if the limit is already reached.
	Acquire(ctx context.Context, key string, limit int) (bool, error)

	// Release frees a previously acquired concurrency slot.
	Release(ctx context.Context, key string) error
}
