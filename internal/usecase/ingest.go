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

	// Enrich metadata with ingestion context (only if caller-provided data absent).
	ingestMeta := map[string]string{
		"ingested_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	if traceID := TraceIDFromContext(ctx); traceID != "" {
		ingestMeta["trace_id"] = traceID
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
