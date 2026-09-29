package middleware

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/gofiber/fiber/v2"
)

// TraceSpanKey is the Fiber locals key under which the request span is stored.
const TraceSpanKey = "_otel_span"

// Tracing returns a Fiber middleware that instruments every HTTP request
// with OpenTelemetry distributed tracing. It extracts W3C TraceContext
// from incoming headers (traceparent/tracestate), starts a server-span,
// and stores the span context in the request context so that usecase
// code can access it via trace.SpanFromContext.
//
// When tracing is disabled, pass a noop tracer — the middleware is
// still a pass-through for any incoming trace context.
func Tracing(tracer trace.Tracer) fiber.Handler {
	if tracer == nil {
		tracer = trace.NewNoopTracerProvider().Tracer("noop")
	}
	return func(c *fiber.Ctx) error {
		// Extract W3C trace context from incoming headers.
		ctx := propagation.TraceContext{}.Extract(c.UserContext(), fiberCarrier{c: c})

		// Start a span for this request.
		ctx, span := tracer.Start(ctx, c.Method()+" "+c.Path(),
			trace.WithAttributes(
				attribute.String("http.method", c.Method()),
				attribute.String("http.target", c.Path()),
				attribute.String("http.scheme", c.Protocol()),
				attribute.String("http.user_agent", string(c.Context().UserAgent())),
			),
		)

		// Set the traced context on the Fiber request context.
		c.SetUserContext(ctx)

		// End the span after the handler completes.
		spanCtx := span
		defer spanCtx.End()

		err := c.Next()

		// Record HTTP status code on the span.
		span.SetAttributes(
			attribute.Int("http.status_code", c.Response().StatusCode()),
		)

		return err
	}
}

// fiberCarrier adapts a Fiber context to propagation.TextMapCarrier
// so the OTel W3C TraceContext propagator can extract traceparent headers.
type fiberCarrier struct {
	c *fiber.Ctx
}

// Ensure fiberCarrier satisfies propagation.TextMapCarrier.
var _ propagation.TextMapCarrier = fiberCarrier{}

func (fc fiberCarrier) Get(key string) string {
	return fc.c.Get(key)
}

func (fc fiberCarrier) Set(key string, value string) {
	fc.c.Set(key, value)
}

func (fc fiberCarrier) Keys() []string {
	return []string{"Traceparent", "Tracestate"}
}
