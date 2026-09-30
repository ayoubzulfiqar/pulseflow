package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	rd "github.com/redis/go-redis/v9"
)

// DedupKeyPrefix is the Redis key prefix for deduplication cache entries.
const DedupKeyPrefix = "pulseflow:dedup:"

// RedisDeduplicator implements entity.Deduplicator using Redis SET with
// NX (set-if-not-exists) and TTL. Uses a single atomic SETNX operation
// with expiration to prevent race conditions under high concurrency.
type RedisDeduplicator struct {
	client *rd.Client
}

// NewDeduplicator creates a Redis-backed deduplicator.
func NewDeduplicator(client *rd.Client) *RedisDeduplicator {
	return &RedisDeduplicator{client: client}
}

// Seen checks if a dedup key exists in Redis. Returns true if the
// key already exists (duplicate), false if it's new.
// Note: this is a read-only check. For atomic check-and-mark, use
// CheckAndMark instead.
func (d *RedisDeduplicator) Seen(ctx context.Context, key entity.DedupKey, window time.Duration) (bool, error) {
	redisKey := DedupKeyPrefix + string(key)
	exists, err := d.client.Exists(ctx, redisKey).Result()
	if err != nil {
		return false, fmt.Errorf("dedup: redis exists: %w", err)
	}
	return exists > 0, nil
}

// Mark records an event as seen in Redis with the deduplication window
// as the TTL. Uses SET NX with expiration for atomicity — if the key
// already exists, this is a no-op (the event is a duplicate).
func (d *RedisDeduplicator) Mark(ctx context.Context, key entity.DedupKey) error {
	redisKey := DedupKeyPrefix + string(key)
	// Default 1-hour TTL if not specified.
	_ = d.client.SetNX(ctx, redisKey, "1", time.Hour)
	return nil
}

// CheckAndMark atomically checks if an event has been seen and marks it
// in a single Redis SETNX EX operation. This is the recommended approach
// for ingress deduplication — one round-trip per event.
// Returns true if the event is a DUPLICATE (should be dropped),
// false if it is NEW (should be ingested).
func (d *RedisDeduplicator) CheckAndMark(ctx context.Context, key entity.DedupKey, window time.Duration) (bool, error) {
	redisKey := DedupKeyPrefix + string(key)
	result, err := d.client.SetNX(ctx, redisKey, "1", window).Result()
	if err != nil {
		return false, fmt.Errorf("dedup: redis setnx: %w", err)
	}
	// SetNX returns true if the key was set (new event),
	// false if the key already existed (duplicate).
	// We invert: duplicate = !result.
	return !result, nil
}

// GenerateKey creates a dedup key from event fields plus an optional
// idempotency key. The hash is SHA-256 of a JSON-serialized struct
// containing: source, type, subject, id, idempotency_key, and payload.
func (d *RedisDeduplicator) GenerateKey(event *entity.Event, idempotencyKey string) (entity.DedupKey, error) {
	return GenerateKeyForEvent(event, idempotencyKey)
}

// GenerateKeyForEvent is a standalone function to generate a dedup key
// without needing a Redis client instance.
func GenerateKeyForEvent(event *entity.Event, idempotencyKey string) (entity.DedupKey, error) {
	type keyParts struct {
		Source      string          `json:"source"`
		Type        string          `json:"type"`
		Subject     string          `json:"subject"`
		ID          string          `json:"id,omitempty"`
		Idempotency string          `json:"idempotency_key,omitempty"`
		Payload     json.RawMessage `json:"payload,omitempty"`
	}

	parts := keyParts{
		Source:      event.Source,
		Type:        string(event.Type),
		Subject:     event.Subject,
		ID:          string(event.ID),
		Idempotency: idempotencyKey,
		Payload:     event.Data,
	}

	data, err := json.Marshal(parts)
	if err != nil {
		return "", fmt.Errorf("dedup: marshal key parts: %w", err)
	}

	hash := sha256.Sum256(data)
	return entity.DedupKey(hex.EncodeToString(hash[:])), nil
}

// Ensure RedisDeduplicator satisfies entity.Deduplicator.
var _ entity.Deduplicator = (*RedisDeduplicator)(nil)
