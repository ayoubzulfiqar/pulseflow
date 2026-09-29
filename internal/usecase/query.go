package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"go.opentelemetry.io/otel/trace"
)

// QueryUseCase handles read-side queries against the event store.
type QueryUseCase struct {
	repo   entity.EventRepository
	logger *slog.Logger
}

// NewQueryUseCase creates a QueryUseCase.
func NewQueryUseCase(repo entity.EventRepository, logger *slog.Logger) *QueryUseCase {
	if logger == nil {
		logger = slog.Default()
	}
	return &QueryUseCase{repo: repo, logger: logger}
}

// GetByID retrieves a single event by its ULID.
func (uc *QueryUseCase) GetByID(ctx context.Context, id entity.EventID) (*entity.Event, error) {
	event, err := uc.repo.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, entity.ErrNotFound) {
			return nil, fmt.Errorf("usecase: %w", entity.ErrNotFound)
		}
		return nil, fmt.Errorf("usecase: query by id: %w", err)
	}
	return event, nil
}

// Query retrieves events matching the filter with pagination.
func (uc *QueryUseCase) Query(ctx context.Context, filter entity.EventFilter) ([]*entity.Event, error) {
	if filter.MaxLimit == 0 {
		filter.MaxLimit = 100
	}
	if filter.MaxLimit > 1000 {
		filter.MaxLimit = 1000
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	events, err := uc.repo.Query(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("usecase: query: %w", err)
	}
	return events, nil
}

// unmarshalStreamMessage converts a Redis Stream message body (map of
// field-value pairs) back into an Event entity.
func unmarshalStreamMessage(msg entity.StreamMessage) (*entity.Event, error) {
	data, ok := msg.Body["data"]
	if !ok {
		return nil, fmt.Errorf("stream message missing 'data' field")
	}

	jsonBytes, err := toJSONBytes(data)
	if err != nil {
		return nil, fmt.Errorf("stream message data to json: %w", err)
	}

	var event entity.Event
	if err := json.Unmarshal(jsonBytes, &event); err != nil {
		return nil, fmt.Errorf("unmarshal event: %w", err)
	}

	// Restore retry count from metadata for DLQ tracking.
	if retryStr, ok := msg.Body["retry_count"]; ok {
		if r, err := strconv.Atoi(fmt.Sprintf("%v", retryStr)); err == nil {
			if event.Metadata == nil {
				event.Metadata = make(map[string]string)
			}
			event.Metadata["retry_count"] = strconv.Itoa(r)
		}
	}

	return &event, nil
}

// getRetryCount extracts the retry count from a stream message body.
func getRetryCount(msg entity.StreamMessage) int {
	if retryStr, ok := msg.Body["retry_count"]; ok {
		if n, err := strconv.Atoi(fmt.Sprintf("%v", retryStr)); err == nil {
			return n
		}
	}
	return 0
}

// toJSONBytes converts the Redis stream field value to a JSON byte slice.
// Redis stores values as strings; the event data is stored as JSON text.
func toJSONBytes(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case []byte:
		return val, nil
	case string:
		return []byte(val), nil
	default:
		return json.Marshal(val)
	}
}

// TraceIDFromContext extracts a trace ID from the context for logging
// correlation. Checks the manual context value first, then falls back
// to the OTel span context if the tracing middleware propagated one.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value("trace_id").(string); ok && v != "" {
		return v
	}
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		return span.SpanContext().TraceID().String()
	}
	return ""
}

// SpanIDFromContext extracts a span ID from the context, falling back
// to the OTel span context when available.
func SpanIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value("span_id").(string); ok && v != "" {
		return v
	}
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		return span.SpanContext().SpanID().String()
	}
	return ""
}

