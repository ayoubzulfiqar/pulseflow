package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
)

// BatchAggregator implements entity.Batcher using an in-process
// buffer with a background flusher. Events are held per destination
// and released as a single batch when either the batch size is reached
// or the max wait timeout elapses.
//
// For distributed deployments, replace this with a Redis Sorted Set-based
// implementation — see internal/adapter/redis/batcher.go.
type BatchAggregator struct {
	logger *slog.Logger

	// buffers holds per-destination pending events.
	buffers map[string]*batchBuffer
	mu      sync.Mutex

	// deliverer sends the batched payload to the webhook URL.
	deliverer entity.WebhookDeliverer

	// flushInterval controls how often the background worker checks
	// for timed-out batches.
	flushInterval time.Duration
}

// batchBuffer holds events for a single destination.
type batchBuffer struct {
	mu       sync.Mutex
	events   []*entity.Event
	capacity int
	timer    *time.Timer
}

// NewBatchAggregator creates a new aggregator with the given deliverer
// and flush interval (how often to check for timed-out batches).
func NewBatchAggregator(deliverer entity.WebhookDeliverer, flushInterval time.Duration, logger *slog.Logger) *BatchAggregator {
	if logger == nil {
		logger = slog.Default()
	}
	if flushInterval <= 0 {
		flushInterval = 1 * time.Second
	}
	ba := &BatchAggregator{
		logger:      logger,
		buffers:     make(map[string]*batchBuffer),
		deliverer:   deliverer,
		flushInterval: flushInterval,
	}
	go ba.runFlusher()
	return ba
}

// Enqueue adds an event to the destination's batch buffer.
// If the batch reaches capacity, it triggers an immediate flush.
func (ba *BatchAggregator) Enqueue(ctx context.Context, destID string, event *entity.Event, rule *entity.BatchingRule) error {
	if rule == nil || !rule.Enabled {
		// No batching — deliver immediately.
		if ba.deliverer != nil {
			ba.deliverer.Deliver(ctx, &entity.Destination{ID: destID}, event)
		}
		return nil
	}

	ba.mu.Lock()
	buf, exists := ba.buffers[destID]
	if !exists {
		buf = &batchBuffer{
			events:   make([]*entity.Event, 0, rule.MaxBatchSize),
			capacity: rule.MaxBatchSize,
			timer:    time.NewTimer(rule.MaxWaitDuration),
		}
		ba.buffers[destID] = buf
	}
	ba.mu.Unlock()

	buf.mu.Lock()
	buf.events = append(buf.events, event)

	// Check if batch is full.
	if len(buf.events) >= buf.capacity {
		buf.mu.Unlock()
		ba.flushBuffer(destID, buf)
	} else {
		buf.mu.Unlock()
	}

	return nil
}

// PendingCount returns the number of events buffered for a destination.
func (ba *BatchAggregator) PendingCount(ctx context.Context, destID string) (int, error) {
	ba.mu.Lock()
	defer ba.mu.Unlock()

	buf, exists := ba.buffers[destID]
	if !exists {
		return 0, nil
	}

	buf.mu.Lock()
	defer buf.mu.Unlock()
	return len(buf.events), nil
}

// Flush releases all buffered events for a destination as a single
// batch payload.
func (ba *BatchAggregator) Flush(ctx context.Context, destID string) ([]*entity.Event, error) {
	ba.mu.Lock()
	buf, exists := ba.buffers[destID]
	ba.mu.Unlock()

	if !exists {
		return nil, nil
	}

	return ba.flushBuffer(destID, buf), nil
}

// flushBuffer delivers buffered events as a batch and returns the events.
// Caller must hold ba.mu when calling this (to remove from map).
func (ba *BatchAggregator) flushBuffer(destID string, buf *batchBuffer) []*entity.Event {
	buf.mu.Lock()
	events := buf.events
	buf.events = make([]*entity.Event, 0, buf.capacity)
	buf.mu.Unlock()

	if len(events) == 0 {
		return nil
	}

	// Reset the timer.
	if !buf.timer.Stop() {
		select {
		case <-buf.timer.C:
		default:
		}
	}
	buf.timer.Reset(0)

	// Build the batch payload.
	payloads := make([]json.RawMessage, 0, len(events))
	for _, e := range events {
		payloads = append(payloads, e.Data)
	}

	batch := &entity.Event{
		Data: json.RawMessage(fmt.Sprintf(`{"events":%s,"count":%d}`,
			mustMarshalJSON(payloads), len(payloads))),
	}

	// Deliver the batch.
	if ba.deliverer != nil {
		// Create a synthetic destination for batch delivery.
		batchDest := &entity.Destination{
			ID:        destID,
			EventPattern: "*",
			URL:       "", // The deliverer should resolve URL from destID.
		}
		ba.deliverer.Deliver(context.Background(), batchDest, batch)
	}

	return events
}

// runFlusher is the background worker that periodically checks for
// timed-out batches and flushes them.
func (ba *BatchAggregator) runFlusher() {
	ticker := time.NewTicker(ba.flushInterval)
	defer ticker.Stop()

	for range ticker.C {
		ba.mu.Lock()
		now := time.Now()
		_ = now
		for destID, buf := range ba.buffers {
			buf.mu.Lock()
			if len(buf.events) > 0 {
				// Check if the timer has expired.
				select {
				case <-buf.timer.C:
					buf.mu.Unlock()
					ba.flushBuffer(destID, buf)
					continue
				default:
				}
			}
			buf.mu.Unlock()
		}
		ba.mu.Unlock()
	}
}

func mustMarshalJSON(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("[]")
	}
	return data
}

// Ensure BatchAggregator satisfies entity.Batcher.
var _ entity.Batcher = (*BatchAggregator)(nil)
