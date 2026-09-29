package entity

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/trace"
)

// TraceKeyPrefix is used when injecting trace context into Event.Metadata.
const (
	TraceIDKey = "trace_id"
	SpanIDKey  = "span_id"
)

// Propagator defines the interface for extracting and injecting W3C trace
// context (traceparent header) across HTTP and stream boundaries.
type Propagator interface {
	// ExtractHTTP extracts trace context from incoming HTTP headers and
	// returns a child context that carries the trace span.
	ExtractHTTP(ctx context.Context, header http.Header) (context.Context, trace.Span)

	// InjectHTTP injects the current trace context into outgoing HTTP headers.
	InjectHTTP(ctx context.Context, header http.Header)

	// FromMetadata extracts trace context from event metadata map
	// (as propagated through Redis Streams).
	FromMetadata(ctx context.Context, metadata map[string]string) (context.Context, trace.Span)

	// ToMetadata injects the current trace context into a metadata map
	// for Redis Stream propagation.
	ToMetadata(ctx context.Context, metadata map[string]string)
}

// Tracer is an alias for the OTel trace.Tracer, used by usecase layers
// for creating child spans during processing.
type Tracer = trace.Tracer

// TraceIDFromContext retrieves the trace ID string from a context
// that may have been populated by ExtractHTTP or FromMetadata.
func TraceIDFromContext(ctx context.Context) string {
	if span := trace.SpanFromContext(ctx); span != nil && span.SpanContext().IsValid() {
		return span.SpanContext().TraceID().String()
	}
	return ""
}
