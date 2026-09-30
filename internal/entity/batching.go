package entity

import (
	"context"
	"encoding/json"
	"time"
)

// BatchingRule configures how events are aggregated before delivery.
// When enabled, matching events are held in a Redis sorted-set buffer
// and released as a single JSON array payload when either the batch
// size is reached or the max wait duration elapses.
type BatchingRule struct {
	// Enabled controls whether batching is active for this destination.
	Enabled bool `json:"enabled"`

	// MaxBatchSize is the maximum number of events to aggregate before
	// flushing. If 0, only the time-based flush applies.
	MaxBatchSize int `json:"max_batch_size"`

	// MaxWaitDuration is the maximum time to hold events before flushing
	// regardless of batch size. Must be >= 1 second.
	MaxWaitDuration time.Duration `json:"max_wait_duration"`

	// EventPattern is the CEL-like pattern that selects which events to batch.
	// E.g. "cart.item_added" or "cart.*".
	EventPattern string `json:"event_pattern"`
}

// BatchEvent represents a single event held in the batching buffer.
type BatchEvent struct {
	Event     *Event       `json:"event"`
	EnqueuedAt time.Time   `json:"enqueued_at"`
	Score      float64     `json:"score"` // Redis ZSET score (timestamp)
}

// BatchPayload is the aggregated delivery payload sent as a single
// webhook with a JSON array of events.
type BatchPayload struct {
	Events []json.RawMessage `json:"events"`
	Count  int               `json:"count"`
}

// Batcher is the port (interface) for batching and aggregation.
type Batcher interface {
	// Enqueue adds an event to the batch buffer. If the batch size is
	// reached, it triggers an immediate flush.
	Enqueue(ctx context.Context, destID string, event *Event, rule *BatchingRule) error

	// Flush releases all buffered events for a destination as a single
	// batch payload delivered to the webhook URL.
	Flush(ctx context.Context, destID string) ([]*Event, error)

	// PendingCount returns the number of events currently buffered for a destination.
	PendingCount(ctx context.Context, destID string) (int, error)
}

// SchemaDefinition holds a JSON Schema for validating event payloads.
type SchemaDefinition struct {
	// EventPattern selects which event types this schema applies to.
	// E.g. "user.created" or "order.*" (supports wildcards).
	EventPattern string `json:"event_pattern"`

	// Schema is the raw JSON Schema document (draft 2020-12).
	Schema json.RawMessage `json:"schema"`

	// TenantID scopes the schema to a specific tenant.
	TenantID string `json:"tenant_id,omitempty"`
}

// SchemaValidationResult describes the outcome of a schema check.
type SchemaValidationResult struct {
	Valid  bool     `json:"valid"`
	Errors []string `json:"errors,omitempty"`
}

// SchemaValidator is the port for validating events against JSON schemas.
type SchemaValidator interface {
	// Validate checks the event's Data field against the registered
	// schema(s) for the event's type and source.
	Validate(ctx context.Context, event *Event) *SchemaValidationResult

	// ValidateWithSchema validates the event data against a specific schema.
	ValidateWithSchema(ctx context.Context, event *Event, schema *SchemaDefinition) *SchemaValidationResult

	// RegisterSchema adds or updates a schema for an event type.
	RegisterSchema(ctx context.Context, schema *SchemaDefinition) error

	// GetSchema returns the schema registered for an event type/source.
	GetSchema(ctx context.Context, eventType EventType, source string) (*SchemaDefinition, error)
}

// SchemaViolationDLQReason is the reason used when an event fails
// schema validation.
const SchemaViolationDLQReason = "schema_violation"
