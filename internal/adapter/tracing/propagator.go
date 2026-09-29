package tracing

import (
	"context"
	"net/http"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// OTelPropagator implements entity.Propagator using the OTel SDK's
// W3C TraceContext propagator for HTTP header extraction/injection,
// and manual metadata propagation for Redis Streams.
type OTelPropagator struct{}

// NewOTelPropagator creates a propagator that uses the OTel SDK's
// W3C TraceContext propagator via the global configuration.
func NewOTelPropagator() *OTelPropagator {
	return &OTelPropagator{}
}

// ExtractHTTP extracts trace context from incoming HTTP headers.
func (p *OTelPropagator) ExtractHTTP(ctx context.Context, header http.Header) (context.Context, trace.Span) {
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.HeaderCarrier(header))
	span := trace.SpanFromContext(ctx)
	return ctx, span
}

// InjectHTTP injects the current trace context into outgoing HTTP headers.
func (p *OTelPropagator) InjectHTTP(ctx context.Context, header http.Header) {
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(header))
}

// FromMetadata extracts trace context from event metadata stored in the
// Redis Stream message body. The metadata map should contain "trace_id"
// and "span_id" keys populated during ingestion.
func (p *OTelPropagator) FromMetadata(ctx context.Context, metadata map[string]string) (context.Context, trace.Span) {
	traceID, hasTraceID := metadata[entity.TraceIDKey]
	spanID, hasSpanID := metadata[entity.SpanIDKey]

	if !hasTraceID || !hasSpanID {
		return ctx, trace.SpanFromContext(ctx)
	}

	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	sid, err := trace.SpanIDFromHex(spanID)
	if err != nil {
		return ctx, trace.SpanFromContext(ctx)
	}

	// Reconstruct a span context with the propagated trace/span IDs.
	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})

	ctx = trace.ContextWithRemoteSpanContext(ctx, spanCtx)
	tracer := trace.NewNoopTracerProvider().Tracer("pulseflow/process")
	_, span := tracer.Start(ctx, "process")
	return ctx, span
}

// ToMetadata injects the current trace context into a metadata map
// for Redis Stream propagation. The map is mutated in place.
func (p *OTelPropagator) ToMetadata(ctx context.Context, metadata map[string]string) {
	span := trace.SpanFromContext(ctx)
	if span == nil || !span.SpanContext().IsValid() {
		return
	}
	sc := span.SpanContext()
	if metadata == nil {
		return
	}
	metadata[entity.TraceIDKey] = sc.TraceID().String()
	metadata[entity.SpanIDKey] = sc.SpanID().String()
}

// Ensure OTelPropagator satisfies entity.Propagator.
var _ entity.Propagator = (*OTelPropagator)(nil)
