package entity

import (
	"encoding/json"
	"time"

	"github.com/oklog/ulid/v2"
)

// EventID is a ULID-based identifier for distributed sorting by time.
type EventID string

// EventType classifies the kind of event (e.g. "user.created", "order.placed").
type EventType string

// Event is the core domain entity representing a single immutable event.
// All fields are validated at ingestion time; once stored, events are immutable.
type Event struct {
	// ID is the ULID identifier, sortable by creation time.
	ID EventID `json:"id"`

	// Source identifies the producer/service that emitted the event.
	Source string `json:"source"`

	// Type classifies the event (e.g. "user.created").
	Type EventType `json:"type"`

	// Subject identifies the entity/resource the event pertains to.
	Subject string `json:"subject"`

	// Data is the arbitrary JSON payload of the event.
	Data json.RawMessage `json:"data"`

	// Metadata carries optional key-value context (tracing, auth, etc.).
	Metadata map[string]string `json:"metadata,omitempty"`

	// Timestamp is when the event occurred (event time, not ingestion time).
	Timestamp time.Time `json:"timestamp"`

	// Version is the schema version for this event type (default "v1").
	Version string `json:"version"`
}

// EventFilter defines query parameters for retrieving events.
type EventFilter struct {
	Types     []EventType
	Sources   []string
	Subjects  []string
	From      time.Time
	To        time.Time
	MaxLimit  int
	Offset    int
}

// GenerateID creates a new ULID-based EventID with the current timestamp.
// ULIDs are 26-character strings that are lexicographically sortable by time,
// making them ideal for distributed event ordering.
func GenerateID() EventID {
	return EventID(ulid.MustNew(ulid.Timestamp(time.Now().UTC()), nil).String())
}

// NewEvent creates a new event with ULID generation and defaults applied.
// If the event already has an ID or Timestamp, it is preserved (idempotent re-submit).
func NewEvent(source string, eventType EventType, subject string, data json.RawMessage) *Event {
	now := time.Now().UTC()
	return &Event{
		ID:        EventID(ulid.MustNew(ulid.Timestamp(now), nil).String()),
		Source:    source,
		Type:      eventType,
		Subject:   subject,
		Data:      data,
		Timestamp: now,
		Version:   "v1",
	}
}

// Validate checks the event for required fields and structural correctness.
// Returns an error if any required field is missing or invalid.
func (e *Event) Validate() error {
	if e == nil {
		return ErrEventNil
	}
	if len(e.ID) == 0 {
		return ErrEventIDRequired
	}
	if len(e.Source) == 0 {
		return ErrEventSourceRequired
	}
	if len(e.Type) == 0 {
		return ErrEventTypeRequired
	}
	if len(e.Subject) == 0 {
		return ErrEventSubjectRequired
	}
	if len(e.Data) == 0 {
		return ErrEventDataRequired
	}
	if !json.Valid(e.Data) {
		return ErrEventDataInvalidJSON
	}
	if e.Timestamp.IsZero() {
		return ErrEventTimestampRequired
	}
	if len(e.Version) == 0 {
		e.Version = "v1"
	}
	return nil
}

// EnrichMetadata merges the given key-value pairs into the event's metadata.
// Existing keys are overwritten.
func (e *Event) EnrichMetadata(extra map[string]string) {
	if e.Metadata == nil {
		e.Metadata = make(map[string]string, len(extra))
	}
	for k, v := range extra {
		e.Metadata[k] = v
	}
}

// AckInfo represents a pending acknowledgment for a stream message.
type AckInfo struct {
	Stream   string
	ID       string
	Consumer string
}
