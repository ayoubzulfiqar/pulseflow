package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	rd "github.com/redis/go-redis/v9"
)

const defaultMaxLen = 100_000

// Config configures the Redis Streams event stream.
type Config struct {
	Stream    string
	Group     string
	DLQStream string
	MaxLen    int // approximate max stream length (0 = default 100k)
}

// Stream implements entity.EventStream using Redis Streams with consumer groups.
type Stream struct {
	client    rd.Cmdable
	stream    string
	group     string
	dlqStream string
	maxLen    int
	logger    *slog.Logger
}

// NewStream creates a new Redis Streams-backed event stream.
func NewStream(client rd.Cmdable, cfg Config, logger *slog.Logger) *Stream {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.MaxLen <= 0 {
		cfg.MaxLen = defaultMaxLen
	}
	return &Stream{
		client:    client,
		stream:    cfg.Stream,
		group:     cfg.Group,
		dlqStream: cfg.DLQStream,
		maxLen:    cfg.MaxLen,
		logger:    logger,
	}
}

// Close is a no-op; the caller owns the Redis client lifecycle.
// Provided for interface satisfaction and future cleanup hooks.
func (s *Stream) Close() error { return nil }

// Publish writes an event to the stream with approximate trimming.
func (s *Stream) Publish(ctx context.Context, event *entity.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("redis: marshal event: %w", err)
	}

	err = s.client.XAdd(ctx, &rd.XAddArgs{
		Stream: s.stream,
		Values: map[string]interface{}{
			"data":     string(data),
			"event_id": string(event.ID),
			"source":   event.Source,
			"type":     string(event.Type),
		},
		MaxLen: int64(s.maxLen),
		Approx: true,
	}).Err()
	if err != nil {
		return fmt.Errorf("redis: xadd: %w", err)
	}
	return nil
}

// EnsureGroup creates the consumer group if it doesn't already exist.
// Idempotent — safe to call on every startup. The stream is created
// automatically via XGROUP CREATE MKSTREAM.
func (s *Stream) EnsureGroup(ctx context.Context) error {
	err := s.client.XGroupCreateMkStream(ctx, s.stream, s.group, "0").Err()
	if err != nil {
		// BUSYGROUP means the group already exists — that's expected.
		if strings.Contains(err.Error(), "BUSYGROUP") {
			return nil
		}
		return fmt.Errorf("redis: xgroup create: %w", err)
	}
	return nil
}

// Consume starts a long-poll consumer loop. It reads batches of new messages
// from the stream via XREADGROUP, invokes the handler for each message, and
// blocks until ctx is cancelled. The handler is responsible for acking
// (on success) or routing to DLQ (on failure).
func (s *Stream) Consume(ctx context.Context, consumerName string, batchSize int, handler entity.StreamHandler) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		streams, err := s.client.XReadGroup(ctx, &rd.XReadGroupArgs{
			Group:    s.group,
			Consumer: consumerName,
			Streams:  []string{s.stream, ">"},
			Count:    int64(batchSize),
			Block:    5 * time.Second,
			NoAck:    false, // messages enter PEL for reliable ack/claim
		}).Result()

		if err != nil {
			if err == rd.Nil {
				// No messages within block window; poll again.
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("redis: xreadgroup: %w", err)
		}

		for _, stream := range streams {
			for _, msg := range stream.Messages {
				sm := entity.StreamMessage{
					ID:       msg.ID,
					Stream:   s.stream,
					Consumer: consumerName,
					Body:     msg.Values,
				}

				if hErr := handler(ctx, sm); hErr != nil {
					// The handler already routed the message to DLQ on
					// failure. Log the error for observability.
					s.logger.Warn("redis: consumer handler error",
						"error", hErr, "stream", s.stream, "id", msg.ID,
						"consumer", consumerName)
				}
			}
		}
	}
}

// Ack acknowledges one or more messages as successfully processed.
func (s *Stream) Ack(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	res := s.client.XAck(ctx, s.stream, s.group, ids...)
	if err := res.Err(); err != nil {
		return fmt.Errorf("redis: xack: %w", err)
	}
	return nil
}

// ClaimStaleMessages rescues pending messages from consumers that have
// gone idle, providing automatic failover. Only messages from OTHER
// consumers (not the current one) are claimed.
func (s *Stream) ClaimStaleMessages(ctx context.Context, consumerName string, minIdle string, batchSize int) ([]entity.StreamMessage, error) {
	idleMs, err := strconv.ParseInt(minIdle, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("redis: parse min_idle: %w", err)
	}
	idleDur := time.Duration(idleMs) * time.Millisecond

	// Fetch all pending entries for this consumer group.
	pending, err := s.client.XPendingExt(ctx, &rd.XPendingExtArgs{
		Stream: s.stream,
		Group:  s.group,
		Count:  int64(batchSize),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: xpendingext: %w", err)
	}

	// Collect IDs from other consumers that have been idle too long.
	var ids []string
	for _, p := range pending {
		if p.Idle >= idleDur && p.Consumer != consumerName {
			ids = append(ids, p.ID)
			if len(ids) >= batchSize {
				break
			}
		}
	}

	if len(ids) == 0 {
		return nil, nil
	}

	// Claim the stale messages on behalf of this consumer.
	claimed, err := s.client.XClaim(ctx, &rd.XClaimArgs{
		Stream:   s.stream,
		Group:    s.group,
		Consumer: consumerName,
		MinIdle:  idleDur,
		Messages: ids,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: xclaim: %w", err)
	}

	result := make([]entity.StreamMessage, len(claimed))
	for i, msg := range claimed {
		result[i] = entity.StreamMessage{
			ID:       msg.ID,
			Stream:   s.stream,
			Consumer: consumerName,
			Body:     msg.Values,
		}
	}
	return result, nil
}

// DeadLetterQueue moves failed messages to the DLQ stream and acks them
// from the main stream so they don't get re-delivered.
func (s *Stream) DeadLetterQueue(ctx context.Context, messages []entity.StreamMessage, reason string) error {
	if len(messages) == 0 {
		return nil
	}

	pipe := s.client.TxPipeline()
	for _, msg := range messages {
		retryCount := getRetryCount(msg.Body) + 1

		// Build DLQ entry, preserving the original event payload.
		dlqValues := buildDLQValues(msg.Body, reason, retryCount)

		pipe.XAdd(ctx, &rd.XAddArgs{
			Stream: s.dlqStream,
			Values: dlqValues,
		})
		pipe.XAck(ctx, s.stream, s.group, msg.ID)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis: dlq pipeline: %w", err)
	}
	return nil
}

// getRetryCount extracts the retry counter from a stream message body.
func getRetryCount(body map[string]interface{}) int {
	if v, ok := body["retry_count"]; ok {
		if n, err := strconv.Atoi(fmt.Sprintf("%v", v)); err == nil {
			return n
		}
	}
	return 0
}

// buildDLQValues constructs the DLQ entry values from the original message.
func buildDLQValues(body map[string]interface{}, reason string, retryCount int) map[string]interface{} {
	values := map[string]interface{}{
		"reason":      reason,
		"retry_count": strconv.Itoa(retryCount),
		"failed_at":   time.Now().UTC().Format(time.RFC3339Nano),
	}
	// Copy original fields for traceability.
	for k, v := range body {
		if _, exists := values[k]; !exists {
			values[k] = v
		}
	}
	return values
}

// PurgeDLQ removes all messages from the dead-letter queue stream.
// Returns the number of entries deleted.
func (s *Stream) PurgeDLQ(ctx context.Context) (int, error) {
	// XRANGE to get all IDs, then XDEL them.
	msgs, err := s.client.XRange(ctx, s.dlqStream, "-", "+").Result()
	if err != nil {
		return 0, fmt.Errorf("redis: xrange dlq: %w", err)
	}

	if len(msgs) == 0 {
		return 0, nil
	}

	ids := make([]string, len(msgs))
	for i, msg := range msgs {
		ids[i] = msg.ID
	}

	deleted, err := s.client.XDel(ctx, s.dlqStream, ids...).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: xdel dlq: %w", err)
	}

	return int(deleted), nil
}

// ListDLQ reads messages from the dead-letter queue stream via XRANGE.
// When limit > 0, at most `limit` messages are returned. When offset > 0,
// the first `offset` messages are skipped.
func (s *Stream) ListDLQ(ctx context.Context, limit, offset int) ([]entity.StreamMessage, error) {
	count := int64(limit)
	if count <= 0 {
		count = 1000 // default page size
	}

	// XRangeN with count for efficient pagination.
	msgs, err := s.client.XRangeN(ctx, s.dlqStream, "-", "+", count).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: xrange dlq: %w", err)
	}

	// Skip offset.
	if offset > 0 && offset < len(msgs) {
		msgs = msgs[offset:]
	} else if offset >= len(msgs) {
		return nil, nil
	}

	// Apply limit after offset.
	if limit > 0 && len(msgs) > limit {
		msgs = msgs[:limit]
	}

	result := make([]entity.StreamMessage, len(msgs))
	for i, msg := range msgs {
		result[i] = entity.StreamMessage{
			ID:       msg.ID,
			Stream:   s.dlqStream,
			Consumer: "", // DLQ messages are not tied to a consumer
			Body:     msg.Values,
		}
	}

	return result, nil
}

// RequeueDLQ moves messages from the DLQ stream back to the main stream.
// It reads each message from the DLQ, re-publishes it to the main stream
// with retry_count reset to 0, and then deletes it from the DLQ.
func (s *Stream) RequeueDLQ(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	// Fetch the DLQ messages via XRange to get all bodies.
	msgs, err := s.client.XRange(ctx, s.dlqStream, "-", "+").Result()
	if err != nil {
		return fmt.Errorf("redis: xrange dlq for requeue: %w", err)
	}

	// Build a set of requested IDs.
	requested := make(map[string]bool, len(ids))
	for _, id := range ids {
		requested[id] = true
	}

	pipe := s.client.TxPipeline()
	queuedCount := 0

	for _, msg := range msgs {
		if !requested[msg.ID] {
			continue
		}

		// Copy the body, reset retry_count to 0.
		values := make(map[string]interface{}, len(msg.Values))
		for k, v := range msg.Values {
			if k == "retry_count" {
				values[k] = "0"
			} else {
				values[k] = v
			}
		}

		// Re-publish to the main stream.
		pipe.XAdd(ctx, &rd.XAddArgs{
			Stream: s.stream,
			Values: values,
			MaxLen: int64(s.maxLen),
			Approx: true,
		})

		// Delete from the DLQ.
		pipe.XDel(ctx, s.dlqStream, msg.ID)
		queuedCount++
	}

	if queuedCount == 0 {
		return nil
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis: requeue dlq pipeline: %w", err)
	}

	s.logger.Info("dlq: requeued messages", "count", queuedCount)
	return nil
}

// partitionNamePattern validates generated partition names to prevent injection.
var partitionNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
