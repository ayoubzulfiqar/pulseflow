package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
)

// EventReplayUseCase implements the time-travel replay engine. It queries
// historical events from the PostgreSQL event store within a given time
// window and publishes them back to the Redis Streams topic for reprocessing.
//
// Events are NOT re-inserted into PostgreSQL — only re-published to the
// stream. This avoids duplicate historical records while allowing the full
// processing pipeline to re-execute on the replayed events.
type EventReplayUseCase struct {
	stream entity.EventStream
	repo   entity.EventRepository
	logger *slog.Logger
}

// NewEventReplayUseCase creates an EventReplayUseCase.
func NewEventReplayUseCase(stream entity.EventStream, repo entity.EventRepository, logger *slog.Logger) *EventReplayUseCase {
	if logger == nil {
		logger = slog.Default()
	}
	return &EventReplayUseCase{stream: stream, repo: repo, logger: logger}
}

// ReplayRequest defines the parameters for a time-travel replay operation.
type ReplayRequest struct {
	// From is the start of the time window (inclusive).
	From time.Time
	// To is the end of the time window (inclusive).
	To time.Time
	// Types filters events to specific types. Empty = all types.
	Types []entity.EventType
	// Sources filters events to specific sources. Empty = all sources.
	Sources []string
	// Subjects filters events to specific subjects. Empty = all subjects.
	Subjects []string
	// MaxEvents caps the number of events to replay. 0 = no cap (capped at 10000).
	MaxEvents int
}

// ReplayResult summarizes a replay operation.
type ReplayResult struct {
	// Replayed is the total number of events selected for replay.
	Replayed int `json:"replayed"`
	// Successful is the count of events successfully republished to the stream.
	Successful int `json:"successful"`
	// Failed is the count of events that could not be published.
	Failed int `json:"failed"`
	// Errors records per-event failure details.
	Errors []string `json:"errors,omitempty"`
}

// Replay queries historical events from the store and re-publishes them
// to the stream. Events that already exist in the stream will be delivered
// to consumers (exactly-once processing must be handled by downstream
// processors via idempotent operations).
func (uc *EventReplayUseCase) Replay(ctx context.Context, req ReplayRequest) (*ReplayResult, error) {
	start := time.Now()

	if req.From.IsZero() && req.To.IsZero() && len(req.Types) == 0 && len(req.Sources) == 0 && len(req.Subjects) == 0 {
		return nil, fmt.Errorf("usecase: replay requires at least one filter or time range")
	}

	maxEvents := req.MaxEvents
	if maxEvents == 0 {
		maxEvents = 10000
	}

	// Fetch historical events in paginated batches, walking through the
	// result set by offset.
	batchSize := 100
	offset := 0
	result := &ReplayResult{}

	for {
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("usecase: replay cancelled: %w", err)
		}

		filter := entity.EventFilter{
			Types:    req.Types,
			Sources:  req.Sources,
			Subjects: req.Subjects,
			From:     req.From,
			To:       req.To,
			MaxLimit: batchSize,
			Offset:   offset,
		}

		events, err := uc.repo.Query(ctx, filter)
		if err != nil {
			return result, fmt.Errorf("usecase: query historical events: %w", err)
		}

		if len(events) == 0 {
			break
		}

		result.Replayed += len(events)

		for _, event := range events {
			if err := ctx.Err(); err != nil {
				return result, fmt.Errorf("usecase: replay cancelled: %w", err)
			}

			// Mark the event as replayed in metadata so processors
			// can detect and handle it appropriately.
			event.EnrichMetadata(map[string]string{
				"replayed_at":   time.Now().UTC().Format(time.RFC3339Nano),
				"replay_origin": "time_travel",
			})

			if err := uc.stream.Publish(ctx, event); err != nil {
				result.Failed++
				result.Errors = append(result.Errors, fmt.Sprintf("event %s: %v", event.ID, err))
				uc.logger.Warn("replay: publish failed", "event_id", event.ID, "error", err)
				continue
			}

			result.Successful++
		}

		offset += len(events)

		if len(events) < batchSize {
			break
		}
		if offset >= maxEvents {
			break
		}
	}

	uc.logger.Info("replay completed",
		"replayed", result.Replayed, "successful", result.Successful,
		"failed", result.Failed, "duration_ms", time.Since(start).Milliseconds())

	return result, nil
}
