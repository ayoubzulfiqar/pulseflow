package redis

import (
	"log/slog"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/sony/gobreaker"
)

// CircuitBreakerStateAdapter exposes a gobreaker's runtime state through
// the entity.CircuitBreakerState interface, enabling admin endpoints to
// query and reset the breaker.
type CircuitBreakerStateAdapter struct {
	name   string
	cb     *gobreaker.CircuitBreaker
	logger *slog.Logger
}

// NewCircuitBreakerStateAdapter wraps a gobreaker for state inspection.
func NewCircuitBreakerStateAdapter(name string, cb *gobreaker.CircuitBreaker, logger *slog.Logger) *CircuitBreakerStateAdapter {
	if logger == nil {
		logger = slog.Default()
	}
	return &CircuitBreakerStateAdapter{name: name, cb: cb, logger: logger}
}

// Name returns the circuit breaker identifier.
func (a *CircuitBreakerStateAdapter) Name() string {
	return a.name
}

// State returns the current breaker state as a string.
func (a *CircuitBreakerStateAdapter) State() string {
	return a.cb.State().String()
}

// Failing returns the current consecutive failure count.
func (a *CircuitBreakerStateAdapter) Failing() int {
	return int(a.cb.Counts().ConsecutiveFailures)
}

// TotalCalls returns the total number of calls since last reset.
func (a *CircuitBreakerStateAdapter) TotalCalls() int {
	return int(a.cb.Counts().Requests)
}

// Reset manually resets the circuit breaker. Sony/gobreaker does not expose
// a Reset method; instead the breaker transitions to half-open automatically
// after the configured timeout. This method is a no-op to satisfy the
// interface, and the admin endpoint's reset function is wired directly in
// main.go via CBResetFn.
func (a *CircuitBreakerStateAdapter) Reset() {
	a.logger.Info("circuit breaker reset requested; gobreaker auto-resets after timeout",
		"name", a.name, "state", a.State())
}

// Ensure CircuitBreakerStateAdapter satisfies entity.CircuitBreakerState.
var _ entity.CircuitBreakerState = (*CircuitBreakerStateAdapter)(nil)
