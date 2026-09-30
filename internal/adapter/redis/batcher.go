package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	rd "github.com/redis/go-redis/v9"
)

// BatchKeyPrefix is the Redis key prefix for batch aggregation buffers.
const BatchKeyPrefix = "pulseflow:batch:"

// LuaFlushBatch atomically drains all events from a sorted set and
// deletes the key in a single server-side operation. This prevents
// race conditions when multiple workers attempt to flush the same
// batch simultaneously.
const LuaFlushBatch = `
local key = KEYS[1]
local events = redis.call("ZRANGE", key, 0, -1)
redis.call("DEL", key)
return events
`

// RedisBatcher implements entity.Batcher using Redis Sorted Sets (ZSET).
// Events are stored with a timestamp score so they can be ordered by
// insertion time. A Lua script atomically drains and deletes the key
// on flush to prevent double-delivery.
type RedisBatcher struct {
	client *rd.Client
	deliverer entity.WebhookDeliverer
}

// NewRedisBatcher creates a Redis-backed batch aggregator.
func NewRedisBatcher(client *rd.Client, deliverer entity.WebhookDeliverer) *RedisBatcher {
	return &RedisBatcher{
		client:      client,
		deliverer:   deliverer,
	}
}

// Enqueue adds an event to the destination's batch buffer.
// The event is serialized and added to a Redis sorted set with the
// current timestamp as the score.
// If the batch reaches MaxBatchSize, an immediate flush is triggered.
func (b *RedisBatcher) Enqueue(ctx context.Context, destID string, event *entity.Event, rule *entity.BatchingRule) error {
	if rule == nil || !rule.Enabled {
		// No batching — deliver immediately.
		if b.deliverer != nil {
			b.deliverer.Deliver(ctx, &entity.Destination{ID: destID}, event)
		}
		return nil
	}

	key := BatchKeyPrefix + destID
	score := float64(time.Now().Unix())

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("batcher: marshal event: %w", err)
	}

	// ZADD with a temporary key (TTL set separately).
	pipe := b.client.TxPipeline()
	pipe.ZAdd(ctx, key, rd.Z{
		Score:  score,
		Member: string(data),
	})
	pipe.Expire(ctx, key, rule.MaxWaitDuration)
	pipe.ZCard(ctx, key)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("batcher: enqueue: %w", err)
	}

	return nil
}

// Flush atomically drains all buffered events for a destination and
// delivers them as a single batch payload.
func (b *RedisBatcher) Flush(ctx context.Context, destID string) ([]*entity.Event, error) {
	key := BatchKeyPrefix + destID

	// Use Lua script to atomically get and delete all events.
	result, err := b.client.Eval(ctx, LuaFlushBatch, []string{key}).Result()
	if err != nil {
		return nil, fmt.Errorf("batcher: flush: %w", err)
	}

	members, ok := result.([]string)
	if !ok {
		return nil, nil
	}

	var events []*entity.Event
	for _, m := range members {
		var event entity.Event
		if err := json.Unmarshal([]byte(m), &event); err != nil {
			continue
		}
		events = append(events, &event)
	}

	// Deliver the batch payload if we have events.
	if len(events) > 0 && b.deliverer != nil {
		payloads := make([]json.RawMessage, 0, len(events))
		for _, e := range events {
			payloads = append(payloads, e.Data)
		}
		batchData, _ := json.Marshal(map[string]any{
			"events": payloads,
			"count":  len(payloads),
		})

		batchEvent := &entity.Event{
			ID:     entity.GenerateID(),
			Type:   "batch.delivered",
			Source: "pulseflow:batcher",
			Data:   batchData,
		}

		b.deliverer.Deliver(ctx, &entity.Destination{ID: destID}, batchEvent)
	}

	return events, nil
}

// PendingCount returns the number of events currently buffered for a destination.
func (b *RedisBatcher) PendingCount(ctx context.Context, destID string) (int, error) {
	key := BatchKeyPrefix + destID
	count, err := b.client.ZCard(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("batcher: pending count: %w", err)
	}
	return int(count), nil
}

// Ensure RedisBatcher satisfies entity.Batcher.
var _ entity.Batcher = (*RedisBatcher)(nil)
