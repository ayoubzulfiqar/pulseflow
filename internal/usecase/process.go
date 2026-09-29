package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
)

const MaxDLQRetries = 5

// EventProcessor defines business-logic processing applied to each event
// after it is consumed from the stream. The default implementation
// (StoreProcessor) simply persists the event; users can plug in custom
// processors for enrichment, routing, or side effects.
type EventProcessor interface {
	Process(ctx context.Context, event *entity.Event) error
}

// StoreProcessor is the default EventProcessor — it persists events to
// the EventRepository.
type StoreProcessor struct {
	repo entity.EventRepository
}

// NewStoreProcessor creates a processor that persists events to the given repo.
func NewStoreProcessor(repo entity.EventRepository) *StoreProcessor {
	return &StoreProcessor{repo: repo}
}

// Process stores the event in PostgreSQL.
func (p *StoreProcessor) Process(ctx context.Context, event *entity.Event) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("processor: validate: %w", err)
	}
	return p.repo.Store(ctx, event)
}

// ProcessUseCase runs consumer-group workers that read events from the stream,
// apply processing logic, acknowledge successes, and route failures to the DLQ.
type ProcessUseCase struct {
	stream       entity.EventStream
	repo         entity.EventRepository
	processor    EventProcessor
	logger       *slog.Logger
	metrics      *Metrics
	consumerName string
	concurrency  int
	batchSize    int
	maxPending   int
	claimMinIdle time.Duration
	running      int32 // atomic

	// Optional: webhook delivery after successful processing.
	destinations entity.DestinationRepository
	filterer     entity.CELFilterer
	deliverer    entity.WebhookDeliverer
	tracer       entity.Tracer
}

// NewProcessUseCase creates a ProcessUseCase.
// The destinations, filterer, deliverer, and tracer parameters are optional —
// pass nil to disable CEL-filtered webhook delivery and tracing.
func NewProcessUseCase(
	stream entity.EventStream,
	repo entity.EventRepository,
	processor EventProcessor,
	consumerName string,
	concurrency int,
	batchSize int,
	maxPending int,
	claimMinIdle time.Duration,
	destinations entity.DestinationRepository,
	filterer entity.CELFilterer,
	deliverer entity.WebhookDeliverer,
	tracer entity.Tracer,
	logger *slog.Logger,
	metrics *Metrics,
) *ProcessUseCase {
	if logger == nil {
		logger = slog.Default()
	}
	return &ProcessUseCase{
		stream:        stream,
		repo:          repo,
		processor:     processor,
		logger:        logger,
		metrics:       metrics,
		consumerName:  consumerName,
		concurrency:   concurrency,
		batchSize:     batchSize,
		maxPending:    maxPending,
		claimMinIdle:  claimMinIdle,
		destinations:  destinations,
		filterer:      filterer,
		deliverer:     deliverer,
		tracer:        tracer,
	}
}

// Run starts the consumer workers and blocks until ctx is cancelled
// or a fatal error occurs. It handles graceful shutdown: in-flight
// messages are finished before returning.
func (uc *ProcessUseCase) Run(ctx context.Context) error {
	// Ensure the consumer group exists before starting workers.
	if err := uc.stream.EnsureGroup(ctx); err != nil {
		return fmt.Errorf("process: ensure group: %w", err)
	}

	atomic.StoreInt32(&uc.running, 1)
	defer atomic.StoreInt32(&uc.running, 0)

	var wg sync.WaitGroup
	for i := 0; i < uc.concurrency; i++ {
		consumerID := fmt.Sprintf("%s-%d", uc.consumerName, i)
		wg.Add(1)
		go func(cid string) {
			defer wg.Done()
			uc.consumeLoop(ctx, cid)
		}(consumerID)
	}

	// Start the XCLAIM failover ticker.
	claimTick := time.NewTicker(uc.claimMinIdle / 2)
	defer claimTick.Stop()

	for {
		select {
		case <-ctx.Done():
			uc.logger.Info("process: shutting down consumers", "active", atomic.LoadInt32(&uc.running))
			wg.Wait()
			return ctx.Err()
		case <-claimTick.C:
			uc.claimStale(ctx)
		}
	}
}

// consumeLoop runs the main consume-ack-dlq cycle for a single consumer.
func (uc *ProcessUseCase) consumeLoop(ctx context.Context, consumerName string) {
	handler := func(ctx context.Context, msg entity.StreamMessage) error {
		return uc.handleMessage(ctx, msg)
	}

	// Consume blocks internally with a polling interval. When ctx is
	// cancelled, Consume returns and the loop exits.
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := uc.stream.Consume(ctx, consumerName, uc.batchSize, handler); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			uc.logger.Error("process: consume error", "consumer", consumerName, "error", err)
			// Back off before retrying to avoid tight error loops.
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}

// handleMessage processes a single stream message: unmarshal, process,
// ack on success, or route to DLQ on failure.
func (uc *ProcessUseCase) handleMessage(ctx context.Context, msg entity.StreamMessage) error {
	// Extract trace context from stream message metadata and start a
	// child span for the entire processing pipeline.
	processCtx := ctx
	if uc.tracer != nil {
		processCtx, _ = uc.tracer.Start(ctx, "process.event")
	}

	// Inject trace metadata into the message body so downstream consumers
	// can link spans.
	traceMeta := extractTraceMetadata(msg.Body)

	start := time.Now()

	event, err := unmarshalStreamMessage(msg)
	if err != nil {
		uc.logger.Error("process: unmarshal stream message", "error", err)
		// Can't parse — send to DLQ.
		reason := fmt.Sprintf("unmarshal: %v", err)
		_ = uc.stream.DeadLetterQueue(ctx, []entity.StreamMessage{msg}, reason)
		uc.persistDLQ(ctx, msg, reason, 1, entity.DLQStatusLocked)
		if uc.metrics != nil {
			uc.metrics.DLQ.Inc()
		}
		return nil // don't re-throw; message is handled (via DLQ)
	}

	// Enrich event metadata with trace context for downstream propagation.
	if traceMeta != nil {
		event.EnrichMetadata(traceMeta)
	}

	if err := uc.processor.Process(processCtx, event); err != nil {
		retryCount := getRetryCount(msg)
		if retryCount >= MaxDLQRetries {
			// Permanently locked — route to DLQ with a terminal reason.
			_ = uc.stream.DeadLetterQueue(ctx, []entity.StreamMessage{msg}, fmt.Sprintf("max retries exceeded: %v", err))
			// Persist to PostgreSQL for audit.
			uc.persistDLQ(ctx, msg, fmt.Sprintf("max retries exceeded: %v", err), retryCount+1, entity.DLQStatusLocked)
			if uc.metrics != nil {
				uc.metrics.DLQLocked.Inc()
			}
			uc.logger.Error("process: event locked in DLQ after max retries",
				"event_id", event.ID, "retry_count", retryCount, "error", err)
			return nil
		}
		// Temporary failure — re-queue to the stream with increased retry count.
		reason := fmt.Sprintf("retry %d: %v", retryCount+1, err)
		_ = uc.stream.DeadLetterQueue(ctx, []entity.StreamMessage{msg}, reason)
		// Persist to PostgreSQL for audit.
		uc.persistDLQ(ctx, msg, reason, retryCount+1, entity.DLQStatusPending)
		uc.logger.Warn("process: event requeued to DLQ for retry",
			"event_id", event.ID, "retry_count", retryCount, "error", err)
		if uc.metrics != nil {
			uc.metrics.DLQ.Inc()
		}
		return nil
	}

	// Success — ack the message.
	if err := uc.stream.Ack(ctx, []string{msg.ID}); err != nil {
		uc.logger.Error("process: ack failed", "error", err, "stream", msg.Stream, "id", msg.ID)
		return err // let the consumer retry
	}

	// --- CEL-filtered webhook delivery (Phase 3) ---
	// After successful processing, look up matching destinations,
	// evaluate CEL filters, and deliver webhooks for matching events.
	uc.deliverWebhooks(processCtx, event)

	if uc.metrics != nil {
		uc.metrics.Processed.WithLabelValues(event.Source, string(event.Type)).Inc()
		uc.metrics.Duration.WithLabelValues("process").Observe(time.Since(start).Seconds())
	}

		uc.logger.Debug("process: event processed",
			"event_id", event.ID, "source", event.Source, "type", event.Type,
			"duration_ms", time.Since(start).Milliseconds(), "trace_id", entity.TraceIDFromContext(processCtx))

	return nil
}

// claimStale rescues pending messages from consumers that have gone idle,
// providing automatic failover when a worker dies.
func (uc *ProcessUseCase) claimStale(ctx context.Context) {
	minIdle := fmt.Sprintf("%d", int(uc.claimMinIdle.Milliseconds()))
	msgs, err := uc.stream.ClaimStaleMessages(ctx, uc.consumerName, minIdle, uc.batchSize)
	if err != nil {
		uc.logger.Error("process: claim stale messages", "error", err)
		return
	}
	if len(msgs) == 0 {
		return
	}
	if uc.metrics != nil {
		uc.metrics.StreamClaims.Add(float64(len(msgs)))
	}
	uc.logger.Info("process: claimed stale messages", "count", len(msgs))

	// Re-deliver claimed messages to the same handler.
	for _, msg := range msgs {
		if err := ctx.Err(); err != nil {
			return
		}
		_ = uc.handleMessage(ctx, msg)
	}
}

// persistDLQ writes a DLQ message to PostgreSQL for audit persistence.
// Failures are logged but non-fatal — the Redis stream is the source of truth.
func (uc *ProcessUseCase) persistDLQ(ctx context.Context, msg entity.StreamMessage, reason string, retryCount int, status entity.DLQStatus) {
	if uc.repo == nil {
		return
	}

	dlqMsg, err := uc.parseDLQFromStream(msg, reason, retryCount, status)
	if err != nil {
		uc.logger.Debug("process: parse dlq for persist", "error", err, "stream_msg_id", msg.ID)
		return
	}

	if err := uc.repo.StoreDLQ(ctx, dlqMsg); err != nil {
		uc.logger.Warn("process: failed to persist dlq to postgres", "error", err, "dlq_id", msg.ID)
	}
}

// deliverWebhooks handles CEL-filtered webhook delivery after successful
// event processing. It looks up matching active destinations, evaluates
// each destination's CEL filter expression against the event, and delivers
// via the webhook sender only for matching destinations. Events with no
// destinations or no CEL filter are passed through.
//
// Per-destination concurrency limiting is handled by the webhook adapter.
// If concurrency is saturated, deliveries are deferred without burning
// stream retry counts.
func (uc *ProcessUseCase) deliverWebhooks(ctx context.Context, event *entity.Event) {
	// Skip webhook delivery if any dependency is missing.
	if uc.deliverer == nil || uc.destinations == nil {
		return
	}

	dests, err := uc.destinations.ListActive(ctx, event.Type, event.Source)
	if err != nil {
		uc.logger.Error("process: list destinations for webhook", "error", err, "event_id", event.ID)
		return
	}
	if len(dests) == 0 {
		return
	}

	traceID := entity.TraceIDFromContext(ctx)
	for _, dest := range dests {
		if dest.IsDisabled() {
			continue
		}

		// Evaluate CEL filter if configured (Phase 3).
		if dest.CELFilter != "" {
			if uc.filterer == nil {
				uc.logger.Warn("process: destination has CEL filter but no filterer configured",
					"dest_id", dest.ID, "event_id", event.ID)
				continue
			}
			ok, err := uc.filterer.ShouldDeliver(ctx, dest.CELFilter, event)
			if err != nil {
				uc.logger.Error("process: cel filter evaluation failed",
					"dest_id", dest.ID, "error", err, "event_id", event.ID)
				continue
			}
			if !ok {
				uc.logger.Debug("process: event filtered out by CEL rule",
					"dest_id", dest.ID, "cel_filter", dest.CELFilter,
					"event_id", event.ID, "trace_id", traceID)
				continue
			}
		}

		uc.logger.Debug("process: delivering webhook",
			"dest_id", dest.ID, "event_id", event.ID, "trace_id", traceID)

		// The webhook sender handles signing, concurrency limiting,
		// retries, and HTTP 410 auto-disable.
		uc.deliverer.Deliver(ctx, dest, event)
	}
}

// extractTraceMetadata pulls trace propagation fields from a stream message's
// body. Returns nil if no trace context is present.
func extractTraceMetadata(body map[string]interface{}) map[string]string {
	if body == nil {
		return nil
	}
	traceID, hasTraceID := body["trace_id"]
	spanID, hasSpanID := body["span_id"]
	if !hasTraceID || !hasSpanID {
		return nil
	}
	return map[string]string{
		"trace_id": fmt.Sprintf("%v", traceID),
		"span_id":  fmt.Sprintf("%v", spanID),
	}
}

// parseDLQFromStream converts a stream message into a DLQMessage entity
// suitable for PostgreSQL persistence.
func (uc *ProcessUseCase) parseDLQFromStream(msg entity.StreamMessage, reason string, retryCount int, status entity.DLQStatus) (*entity.DLQMessage, error) {
	event, err := unmarshalStreamMessage(msg)
	if err != nil {
		return nil, err
	}

	failedAt := time.Now().UTC()
	if v, ok := msg.Body["failed_at"]; ok {
		if t, err := time.Parse(time.RFC3339Nano, fmt.Sprintf("%v", v)); err == nil {
			failedAt = t
		}
	}

	return &entity.DLQMessage{
		ID:         msg.ID,
		Event:      event,
		Reason:     reason,
		RetryCount: retryCount,
		FailedAt:   failedAt,
		Consumer:   msg.Consumer,
		Status:     status,
	}, nil
}
