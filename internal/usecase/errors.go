package usecase

import (
	"errors"
)

// Use case-level errors. These wrap domain errors with use-case context
// so HTTP handlers can map them to appropriate status codes.
var (
	// ErrInvalidEvent is returned when an event fails validation.
	ErrInvalidEvent = errors.New("usecase: event validation failed")

	// ErrStreamPublish is returned when the event cannot be published to the stream.
	ErrStreamPublish = errors.New("usecase: stream publish failed")

	// ErrStreamConsume is returned when the consumer fails to read from the stream.
	ErrStreamConsume = errors.New("usecase: stream consume failed")

	// ErrProcessingFailed is returned when event processing fails after retries.
	ErrProcessingFailed = errors.New("usecase: processing failed")

	// ErrWebhookSend is returned when a webhook delivery fails.
	ErrWebhookSend = errors.New("usecase: webhook send failed")
)
