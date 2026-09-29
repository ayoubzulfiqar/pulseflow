package redis

import (
	"context"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/sony/gobreaker"
)

// CircuitBreakerStream wraps an EventStream with a circuit breaker.
// Publish and EnsureGroup are protected; Consume and passthrough
// methods are left unwrapped because the process use case's consumeLoop
// already implements its own error backoff.
type CircuitBreakerStream struct {
	inner entity.EventStream
	cb    *gobreaker.CircuitBreaker
}

// NewCircuitBreakerStream wraps an EventStream with circuit breaking.
// If cb is nil, the inner stream is returned as-is (no protection).
func NewCircuitBreakerStream(inner entity.EventStream, cb *gobreaker.CircuitBreaker) entity.EventStream {
	if cb == nil {
		return inner
	}
	return &CircuitBreakerStream{inner: inner, cb: cb}
}

func (s *CircuitBreakerStream) Publish(ctx context.Context, event *entity.Event) error {
	_, err := s.cb.Execute(func() (interface{}, error) {
		return nil, s.inner.Publish(ctx, event)
	})
	return err
}

func (s *CircuitBreakerStream) EnsureGroup(ctx context.Context) error {
	_, err := s.cb.Execute(func() (interface{}, error) {
		return nil, s.inner.EnsureGroup(ctx)
	})
	return err
}

func (s *CircuitBreakerStream) Consume(ctx context.Context, consumerName string, batchSize int, handler entity.StreamHandler) error {
	return s.inner.Consume(ctx, consumerName, batchSize, handler)
}

func (s *CircuitBreakerStream) Ack(ctx context.Context, ids []string) error {
	return s.inner.Ack(ctx, ids)
}

func (s *CircuitBreakerStream) ClaimStaleMessages(ctx context.Context, consumerName string, minIdle string, batchSize int) ([]entity.StreamMessage, error) {
	return s.inner.ClaimStaleMessages(ctx, consumerName, minIdle, batchSize)
}

func (s *CircuitBreakerStream) DeadLetterQueue(ctx context.Context, messages []entity.StreamMessage, reason string) error {
	return s.inner.DeadLetterQueue(ctx, messages, reason)
}

func (s *CircuitBreakerStream) ListDLQ(ctx context.Context, limit, offset int) ([]entity.StreamMessage, error) {
	return s.inner.ListDLQ(ctx, limit, offset)
}

func (s *CircuitBreakerStream) RequeueDLQ(ctx context.Context, ids []string) error {
	return s.inner.RequeueDLQ(ctx, ids)
}

func (s *CircuitBreakerStream) PurgeDLQ(ctx context.Context) (int, error) {
	return s.inner.PurgeDLQ(ctx)
}
