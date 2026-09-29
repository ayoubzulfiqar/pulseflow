package tracing

import (
	"context"
	"net/http"
	"testing"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestExtractHTTP_WithValidTraceparent(t *testing.T) {
	// Construct a valid W3C traceparent header.
	// Format: 00-<trace-id>-<span-id>-01
	traceID := "0123456789abcdef0123456789abcdef"
	spanID := "0123456789abcdef"
	traceparent := "00-" + traceID + "-" + spanID + "-01"

	header := http.Header{}
	header.Set("Traceparent", traceparent)

	p := NewOTelPropagator()
	ctx := context.Background()
	newCtx, span := p.ExtractHTTP(ctx, header)

	require.NotNil(t, span)
	sc := span.SpanContext()
	assert.True(t, sc.IsValid())
	assert.Equal(t, traceID, sc.TraceID().String())
	assert.Equal(t, spanID, sc.SpanID().String())

	// Verify the context carries the span context.
	retrievedSpan := trace.SpanFromContext(newCtx)
	assert.True(t, retrievedSpan.SpanContext().IsValid())
}

func TestExtractHTTP_WithoutTraceparent(t *testing.T) {
	header := http.Header{}
	p := NewOTelPropagator()
	ctx := context.Background()

	newCtx, span := p.ExtractHTTP(ctx, header)
	_ = newCtx
	// Without traceparent, the span should be a no-op span.
	assert.NotNil(t, span)
	assert.False(t, span.SpanContext().IsValid())
}

func TestInjectHTTP_SetsTraceparent(t *testing.T) {
	p := NewOTelPropagator()

	// Create a valid span context to inject.
	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0123456789abcdef")
	require.NoError(t, err)

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	header := http.Header{}
	p.InjectHTTP(ctx, header)

	traceparent := header.Get("Traceparent")
	require.NotEmpty(t, traceparent, "traceparent header should be set")
	assert.Contains(t, traceparent, "00-")

	// Verify we can extract back what we injected.
	extractedCtx, extractedSpan := p.ExtractHTTP(context.Background(), header)
	require.True(t, extractedSpan.SpanContext().IsValid())
	assert.Equal(t, traceID.String(), extractedSpan.SpanContext().TraceID().String())
	_ = extractedCtx
}

func TestFromMetadata_WithValidTraceIDs(t *testing.T) {
	p := NewOTelPropagator()

	metadata := map[string]string{
		entity.TraceIDKey: "0123456789abcdef0123456789abcdef",
		entity.SpanIDKey:  "0123456789abcdef",
	}

	ctx := context.Background()
	newCtx, span := p.FromMetadata(ctx, metadata)

	require.NotNil(t, span)
	sc := span.SpanContext()
	assert.True(t, sc.IsValid())
	assert.Equal(t, "0123456789abcdef0123456789abcdef", sc.TraceID().String())
	assert.Equal(t, "0123456789abcdef", sc.SpanID().String())

	// Verify trace ID is retrievable from context.
	traceID := entity.TraceIDFromContext(newCtx)
	assert.Equal(t, "0123456789abcdef0123456789abcdef", traceID)
}

func TestFromMetadata_WithoutTraceIDs(t *testing.T) {
	p := NewOTelPropagator()

	metadata := map[string]string{}
	ctx := context.Background()

	newCtx, span := p.FromMetadata(ctx, metadata)
	_ = newCtx
	assert.NotNil(t, span)
	assert.False(t, span.SpanContext().IsValid())
}

func TestFromMetadata_InvalidTraceID(t *testing.T) {
	p := NewOTelPropagator()

	metadata := map[string]string{
		entity.TraceIDKey: "invalid-trace-id",
		entity.SpanIDKey:  "0123456789abcdef",
	}

	ctx := context.Background()
	_, span := p.FromMetadata(ctx, metadata)

	assert.NotNil(t, span)
	assert.False(t, span.SpanContext().IsValid(), "invalid trace ID should produce invalid span context")
}

func TestToMetadata_WithValidSpan(t *testing.T) {
	p := NewOTelPropagator()

	// Create a real span context.
	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0123456789abcdef")
	require.NoError(t, err)

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     false,
	})

	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	metadata := make(map[string]string)
	p.ToMetadata(ctx, metadata)

	assert.Equal(t, "0123456789abcdef0123456789abcdef", metadata[entity.TraceIDKey])
	assert.Equal(t, "0123456789abcdef", metadata[entity.SpanIDKey])
}

func TestToMetadata_NoValidSpan(t *testing.T) {
	p := NewOTelPropagator()

	ctx := context.Background()
	metadata := make(map[string]string)
	p.ToMetadata(ctx, metadata)

	assert.Empty(t, metadata[entity.TraceIDKey])
	assert.Empty(t, metadata[entity.SpanIDKey])
}

func TestToMetadata_NilMap(t *testing.T) {
	p := NewOTelPropagator()

	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	// Should not panic on nil map.
	p.ToMetadata(ctx, nil)
}

func TestFromMetadataToMetadata_Roundtrip(t *testing.T) {
	p := NewOTelPropagator()

	// Inject metadata.
	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	metadata := make(map[string]string)
	p.ToMetadata(ctx, metadata)

	// Extract from metadata.
	ctx2, span := p.FromMetadata(context.Background(), metadata)
	require.True(t, span.SpanContext().IsValid())
	assert.Equal(t, traceID.String(), span.SpanContext().TraceID().String())
	assert.Equal(t, spanID.String(), span.SpanContext().SpanID().String())

	traceIDStr := entity.TraceIDFromContext(ctx2)
	assert.NotEmpty(t, traceIDStr)
}

func TestOTelPropagator_ImplementsInterface(t *testing.T) {
	var _ entity.Propagator = (*OTelPropagator)(nil)
}
