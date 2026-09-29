package entity

import "errors"

// Domain-level errors. These are sentinel values that callers can check with
// errors.Is. Adapters may return wrapped versions; the underlying sentinel
// allows the use case layer to branch on specific failure modes.
var (
	// ErrEventNil is returned when an event entity is nil.
	ErrEventNil = errors.New("entity: event is nil")

	// ErrEventIDRequired is returned when an event has no ID.
	ErrEventIDRequired = errors.New("entity: event id is required")

	// ErrEventSourceRequired is returned when an event has no source.
	ErrEventSourceRequired = errors.New("entity: event source is required")

	// ErrEventTypeRequired is returned when an event has no type.
	ErrEventTypeRequired = errors.New("entity: event type is required")

	// ErrEventSubjectRequired is returned when an event has no subject.
	ErrEventSubjectRequired = errors.New("entity: event subject is required")

	// ErrEventDataRequired is returned when an event has no data payload.
	ErrEventDataRequired = errors.New("entity: event data is required")

	// ErrEventDataInvalidJSON is returned when the event data is not valid JSON.
	ErrEventDataInvalidJSON = errors.New("entity: event data is not valid JSON")

	// ErrEventTimestampRequired is returned when an event has no timestamp.
	ErrEventTimestampRequired = errors.New("entity: event timestamp is required")

	// ErrDLQEmpty is returned when a DLQ operation targets no messages.
	ErrDLQEmpty = errors.New("entity: dlq operation targets no messages")

	// ErrDLQMessageNotFound is returned when a DLQ message ID does not exist.
	ErrDLQMessageNotFound = errors.New("entity: dlq message not found")

	// ErrDLQOperationNotAllowed is returned when a DLQ operation is invalid
	// for the current message status (e.g. retry on a locked message).
	ErrDLQOperationNotAllowed = errors.New("entity: dlq operation not allowed")
)
