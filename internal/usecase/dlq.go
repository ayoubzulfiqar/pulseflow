package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
)

// DLQUseCase provides operator-level control over dead-lettered messages.
// It coordinates between the Redis stream (source of truth for DLQ entries)
// and the PostgreSQL repository (audit/durable record of DLQ state).
type DLQUseCase struct {
	stream entity.EventStream
	repo   entity.EventRepository
	logger *slog.Logger
}

// NewDLQUseCase creates a DLQUseCase.
func NewDLQUseCase(stream entity.EventStream, repo entity.EventRepository, logger *slog.Logger) *DLQUseCase {
	if logger == nil {
		logger = slog.Default()
	}
	return &DLQUseCase{stream: stream, repo: repo, logger: logger}
}

// ListDLQ retrieves DLQ messages from both the Redis stream and the
// PostgreSQL audit store, merging them. Only the stream is queried if
// the repository is unavailable (degraded mode).
func (uc *DLQUseCase) ListDLQ(ctx context.Context, filter entity.DLQFilter) ([]*entity.DLQMessage, error) {
	if filter.MaxLimit <= 0 {
		filter.MaxLimit = 100
	}
	if filter.MaxLimit > 1000 {
		filter.MaxLimit = 1000
	}

	// Primary: query the Redis DLQ stream directly.
	streamMsgs, err := uc.stream.ListDLQ(ctx, filter.MaxLimit, filter.Offset)
	if err != nil {
		return nil, fmt.Errorf("usecase: list dlq from stream: %w", err)
	}

	results := make([]*entity.DLQMessage, 0, len(streamMsgs))
	for _, sm := range streamMsgs {
		msg, err := parseDLQStreamMessage(sm)
		if err != nil {
			uc.logger.Warn("dlq: failed to parse stream message", "id", sm.ID, "error", err)
			continue
		}

		// Enrich from PostgreSQL if available (authoritative retry_count, status).
		if uc.repo != nil {
			if dbMsg, dbErr := uc.repo.GetDLQByID(ctx, msg.ID); dbErr != nil {
				if !errors.Is(dbErr, entity.ErrDLQMessageNotFound) {
					uc.logger.Debug("dlq: postgres lookup failed", "id", msg.ID, "error", dbErr)
				}
				// Fall back to stream-only data.
			} else {
				// Merge: prioritize DB status/reason but keep stream ID.
				if dbMsg.Status != "" {
					msg.Status = dbMsg.Status
				}
				if dbMsg.FailedAt.After(msg.FailedAt) {
					msg.FailedAt = dbMsg.FailedAt
				}
				if dbMsg.RetryCount > msg.RetryCount {
					msg.RetryCount = dbMsg.RetryCount
				}
			}
		}

		results = append(results, msg)
	}

	// Apply server-side filtering for types/sources/subjects/status.
	results = filterDLQResults(results, filter)

	return results, nil
}

// RetryDLQ re-enqueues selected DLQ messages back to the main stream
// for reprocessing. Retry counts are reset to zero on the re-published
// messages. Messages that are locked (permanently failed) can still be
// retried — the operator explicitly chose this.
func (uc *DLQUseCase) RetryDLQ(ctx context.Context, ids []string) (*DLQRetryResult, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: no message IDs provided", entity.ErrDLQEmpty)
	}

	result := &DLQRetryResult{
		Requested: len(ids),
	}

	// Fetch full DLQ messages so we can re-publish the event payload.
	streamMsgs, err := uc.stream.ListDLQ(ctx, len(ids), 0)
	if err != nil {
		return nil, fmt.Errorf("usecase: fetch dlq for retry: %w", err)
	}

	// Build a map of requested IDs for efficient lookup.
	requested := make(map[string]bool, len(ids))
	for _, id := range ids {
		requested[id] = true
	}

	type requeueItem struct {
		streamID string
		eventID  entity.EventID
	}
	var toRequeue []requeueItem

	for _, sm := range streamMsgs {
		if !requested[sm.ID] {
			continue
		}

		msg, err := parseDLQStreamMessage(sm)
		if err != nil {
			result.Failed = append(result.Failed, DLQRetryFailure{
				ID:    sm.ID,
				Error: fmt.Sprintf("unmarshal: %v", err),
			})
			result.Skipped++
			continue
		}

		// Reset retry count on the event for fresh processing.
		if msg.Event != nil {
			msg.Event.EnrichMetadata(map[string]string{
				"retry_count":  "0",
				"dlq_retried_at": time.Now().UTC().Format(time.RFC3339Nano),
			})
		}

		// Update PostgreSQL audit record.
		if uc.repo != nil {
			status := entity.DLQStatusProcessing
			_ = uc.repo.UpdateDLQStatus(ctx, msg.ID, status, 0)
		}

		toRequeue = append(toRequeue, requeueItem{
			streamID: sm.ID,
			eventID:  msg.Event.ID,
		})
	}

	if len(toRequeue) == 0 {
		return result, nil
	}

	// Requeue via the stream: XADD to main stream + XDEL from DLQ.
	requeueIDs := make([]string, len(toRequeue))
	for i, item := range toRequeue {
		requeueIDs[i] = item.streamID
	}

	if err := uc.stream.RequeueDLQ(ctx, requeueIDs); err != nil {
		return nil, fmt.Errorf("usecase: requeue dlq: %w", err)
	}

	result.Requeued = len(requeueIDs)
	return result, nil
}

// PurgeDLQ removes all messages from the DLQ stream. Optionally archives
// to PostgreSQL before deletion.
func (uc *DLQUseCase) PurgeDLQ(ctx context.Context, archive bool) (int, error) {
	// Optionally archive to Postgres first.
	if archive && uc.repo != nil {
		msgs, err := uc.stream.ListDLQ(ctx, 0, 0)
		if err != nil {
			return 0, fmt.Errorf("usecase: list dlq for archive: %w", err)
		}
		for _, sm := range msgs {
			msg, err := parseDLQStreamMessage(sm)
			if err != nil {
				continue
			}
			_ = uc.repo.StoreDLQ(ctx, msg)
		}
	}

	deleted, err := uc.stream.PurgeDLQ(ctx)
	if err != nil {
		return 0, fmt.Errorf("usecase: purge dlq: %w", err)
	}

	uc.logger.Info("dlq: purged", "count", deleted)
	return deleted, nil
}

// DLQRetryResult summarizes a bulk retry operation.
type DLQRetryResult struct {
	Requested  int               `json:"requested"`
	Requeued   int               `json:"requeued"`
	Skipped    int               `json:"skipped"`
	Failed     []DLQRetryFailure `json:"failed,omitempty"`
}

// DLQRetryFailure records a message that could not be re-enqueued.
type DLQRetryFailure struct {
	ID    string `json:"id"`
	Error string `json:"error"`
}

// parseDLQStreamMessage converts a raw Redis stream message into a DLQMessage.
func parseDLQStreamMessage(msg entity.StreamMessage) (*entity.DLQMessage, error) {
	dlqMsg := &entity.DLQMessage{
		ID:       msg.ID,
		Consumer: msg.Consumer,
		Status:   entity.DLQStatusPending,
	}

	// Extract fields written by buildDLQValues in the stream adapter.
	if v, ok := msg.Body["reason"]; ok {
		dlqMsg.Reason = fmt.Sprintf("%v", v)
	}
	if v, ok := msg.Body["retry_count"]; ok {
		if n, err := strconv.Atoi(fmt.Sprintf("%v", v)); err == nil {
			dlqMsg.RetryCount = n
		}
	}
	if v, ok := msg.Body["failed_at"]; ok {
		if t, err := time.Parse(time.RFC3339Nano, fmt.Sprintf("%v", v)); err == nil {
			dlqMsg.FailedAt = t
		} else {
			dlqMsg.FailedAt = time.Now().UTC()
		}
	}

	// Extract the original event from the "data" field.
	if data, ok := msg.Body["data"]; ok {
		jsonBytes, err := toJSONBytes(data)
		if err != nil {
			return nil, fmt.Errorf("dlq: data to json: %w", err)
		}
		var event entity.Event
		if err := json.Unmarshal(jsonBytes, &event); err != nil {
			return nil, fmt.Errorf("dlq: unmarshal event: %w", err)
		}
		// Preserve retry count in event metadata.
		if event.Metadata == nil {
			event.Metadata = make(map[string]string)
		}
		if _, exists := event.Metadata["retry_count"]; !exists {
			event.Metadata["retry_count"] = strconv.Itoa(dlqMsg.RetryCount)
		}
		dlqMsg.Event = &event
	}

	if dlqMsg.FailedAt.IsZero() {
		dlqMsg.FailedAt = time.Now().UTC()
	}

	// Override status/retry from the "status" field if present.
	if v, ok := msg.Body["status"]; ok {
		switch fmt.Sprintf("%v", v) {
		case "locked":
			dlqMsg.Status = entity.DLQStatusLocked
		case "processing":
			dlqMsg.Status = entity.DLQStatusProcessing
		case "resolved":
			dlqMsg.Status = entity.DLQStatusResolved
		}
	}

	return dlqMsg, nil
}

// filterDLQResults applies server-side filtering for types, sources,
// subjects, and status on a slice of DLQ messages.
func filterDLQResults(msgs []*entity.DLQMessage, filter entity.DLQFilter) []*entity.DLQMessage {
	result := msgs
	if len(filter.Types) > 0 {
		filtered := make([]*entity.DLQMessage, 0, len(result))
		for _, m := range result {
			if m.Event != nil {
				for _, t := range filter.Types {
					if m.Event.Type == t {
						filtered = append(filtered, m)
						break
					}
				}
			}
		}
		result = filtered
	}
	if len(filter.Sources) > 0 {
		filtered := make([]*entity.DLQMessage, 0, len(result))
		for _, m := range result {
			if m.Event != nil {
				for _, s := range filter.Sources {
					if m.Event.Source == s {
						filtered = append(filtered, m)
						break
					}
				}
			}
		}
		result = filtered
	}
	if len(filter.Subjects) > 0 {
		filtered := make([]*entity.DLQMessage, 0, len(result))
		for _, m := range result {
			if m.Event != nil {
				for _, s := range filter.Subjects {
					if m.Event.Subject == s {
						filtered = append(filtered, m)
						break
					}
				}
			}
		}
		result = filtered
	}
	if filter.Status != "" {
		filtered := make([]*entity.DLQMessage, 0, len(result))
		for _, m := range result {
			if m.Status == filter.Status {
				filtered = append(filtered, m)
			}
		}
		result = filtered
	}
	return result
}
