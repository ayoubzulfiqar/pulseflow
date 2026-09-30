package entity

import (
	"context"
	"time"
)

// DedupKey is a content hash used for ingress deduplication.
// It is computed from: source + type + subject + payload + idempotency_key.
type DedupKey string

// DeduplicationConfig configures ingress-level deduplication.
type DeduplicationConfig struct {
	// Window is how long to remember event hashes (e.g. 1h).
	// Events with the same hash arriving within this window are dropped.
	Window time.Duration `json:"window"`

	// Enabled controls whether deduplication is active.
	Enabled bool `json:"enabled"`
}

// Deduplicator is the port for preventing duplicate event ingestion.
type Deduplicator interface {
	// Seen checks if an event with the given key has been ingested
	// within the deduplication window. Returns true if it has been
	// seen (duplicate), false if it is new.
	Seen(ctx context.Context, key DedupKey, window time.Duration) (bool, error)

	// Mark records an event as seen for future deduplication checks.
	Mark(ctx context.Context, key DedupKey) error

	// CheckAndMark atomically checks if the event is a duplicate and
	// marks it in a single operation. Returns true if duplicate.
	CheckAndMark(ctx context.Context, key DedupKey, window time.Duration) (bool, error)

	// GenerateKey creates a dedup key from the event and an optional
	// caller-provided idempotency key.
	GenerateKey(event *Event, idempotencyKey string) (DedupKey, error)
}

// DedupResult describes the outcome of a deduplication check.
type DedupResult struct {
	// Duplicate is true if the event was already seen within the window.
	Duplicate bool

	// Key is the dedup key that was checked.
	Key DedupKey
}
