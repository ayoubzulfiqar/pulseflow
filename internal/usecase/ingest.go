package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
)

// IngestUseCase handles event ingestion: validation, enrichment, stream
// publishing, and optional webhook notification.
type IngestUseCase struct {
	stream  entity.EventStream
	repo    entity.EventRepository
	webhook *WebhookSender
	logger  *slog.Logger
	metrics *Metrics

	// Optional: schema validation and ingress deduplication.
	schemaValidator entity.SchemaValidator
	deduplicator    entity.Deduplicator
	dedupWindow     time.Duration
}

// NewIngestUseCase creates an IngestUseCase. The webhook sender may be nil
// to disable webhook notifications. The repo may be nil if events are only
// published to the stream (no immediate store).
func NewIngestUseCase(
	stream entity.EventStream,
	repo entity.EventRepository,
	webhook *WebhookSender,
	logger *slog.Logger,
	metrics *Metrics,
) *IngestUseCase {
	if logger == nil {
		logger = slog.Default()
	}
	return &IngestUseCase{
		stream:  stream,
		repo:    repo,
		webhook: webhook,
		logger:  logger,
		metrics: metrics,
	}
}

// Ingest validates the event, enriches it with metadata, persists a copy,
// publishes to the stream, and fires webhooks.
func (uc *IngestUseCase) Ingest(ctx context.Context, event *entity.Event) (*entity.Event, error) {
	start := time.Now()
	if event == nil {
		return nil, fmt.Errorf("%w: event is nil", ErrInvalidEvent)
	}

	// Generate ULID if not provided (allows idempotent re-submit with a
	// caller-supplied ID).
	if len(event.ID) == 0 {
		event.ID = entity.GenerateID()
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if len(event.Version) == 0 {
		event.Version = "v1"
	}

	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}

	// --- Ingress Deduplication (exactly-once wedge) ---
	// The idempotency key can be passed via event metadata
	// (set by the HTTP handler from the Idempotency-Key header)
	// or derived from the event's content hash.
	idempotencyKey := event.Metadata["idempotency_key"]
	if uc.deduplicator != nil {
		key, err := uc.deduplicator.GenerateKey(event, idempotencyKey)
		if err == nil {
			isDuplicate, err := uc.deduplicator.CheckAndMark(ctx, key, uc.dedupWindow)
			if err == nil && isDuplicate {
				// Silently return success — the sender's retry is satisfied.
				uc.logger.Debug("event deduplicated (duplicate within window)",
					"event_id", event.ID, "source", event.Source, "type", event.Type)
				return event, nil
			}
		}
	}

	// --- Schema Validation (contract testing wedge) ---
	if uc.schemaValidator != nil {
		result := uc.validateSchema(ctx, event)
		if !result.Valid {
			// Route to schema-violation DLQ.
			uc.logger.Warn("event failed schema validation",
				"event_id", event.ID, "errors", result.Errors)
			if uc.repo != nil {
				dlqMsg := &entity.DLQMessage{
					ID:      string(event.ID),
					Event:   event,
					Reason:  entity.SchemaViolationDLQReason,
					Status:  entity.DLQStatusPending,
					FailedAt: time.Now().UTC(),
				}
				_ = uc.repo.StoreDLQ(ctx, dlqMsg)
			}
			// Return success so the sender isn't penalized — they
			// receive a 200 but the event goes to the DLQ.
			return event, nil
		}
	}

	// Enrich metadata with ingestion context (only if caller-provided data absent).
	ingestMeta := map[string]string{
		"ingested_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	if traceID := TraceIDFromContext(ctx); traceID != "" {
		ingestMeta["trace_id"] = traceID
	}
	if spanID := SpanIDFromContext(ctx); spanID != "" {
		ingestMeta["span_id"] = spanID
	}
	event.EnrichMetadata(ingestMeta)

	// Persist a copy to the durable store before publishing. This provides
	// a replay source if the stream is lost.
	if uc.repo != nil {
		if err := uc.repo.Store(ctx, event); err != nil {
			return nil, fmt.Errorf("usecase: store event: %w", err)
		}
	}

	// Publish to the streaming layer for downstream consumers.
	if err := uc.stream.Publish(ctx, event); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrStreamPublish, err)
	}

	if uc.metrics != nil {
		uc.metrics.Ingested.WithLabelValues(event.Source, string(event.Type)).Inc()
	}

	// Fire webhooks asynchronously — failures don't block ingestion.
	if uc.webhook != nil && len(uc.webhook.endpoints) > 0 {
		uc.webhook.Notify(ctx, event)
	}

	uc.logger.Info("event ingested",
		"event_id", event.ID,
		"source", event.Source,
		"type", event.Type,
		"duration_ms", time.Since(start).Milliseconds())

	return event, nil
}

// WithSchemaValidator enables JSON schema validation before persistence.
// Events that fail validation are sent to the DLQ with SchemaViolationDLQReason.
func (uc *IngestUseCase) WithSchemaValidator(sv entity.SchemaValidator) *IngestUseCase {
	uc.schemaValidator = sv
	return uc
}

// WithDeduplicator enables ingress-level deduplication using a Redis cache.
// Duplicate events (same content hash) within the window are silently dropped
// but still return 200 OK to the caller.
func (uc *IngestUseCase) WithDeduplicator(d entity.Deduplicator, window time.Duration) *IngestUseCase {
	uc.deduplicator = d
	uc.dedupWindow = window
	return uc
}

// checkDedup performs an atomic check-and-mark against the deduplication cache.
// Returns true if the event is a duplicate (should be dropped).
func (uc *IngestUseCase) checkDedup(ctx context.Context, event *entity.Event, idempotencyKey string) bool {
	if uc.deduplicator == nil {
		return false
	}
	key, err := uc.deduplicator.GenerateKey(event, idempotencyKey)
	if err != nil {
		return false
	}
	duplicate, _ := uc.deduplicator.CheckAndMark(ctx, key, uc.dedupWindow)
	return duplicate
}

// validateSchema checks the event against registered JSON schemas.
// Returns nil if validation passes, or an error describing the failures.
func (uc *IngestUseCase) validateSchema(ctx context.Context, event *entity.Event) *entity.SchemaValidationResult {
	if uc.schemaValidator == nil {
		return &entity.SchemaValidationResult{Valid: true}
	}
	return uc.schemaValidator.Validate(ctx, event)
}
