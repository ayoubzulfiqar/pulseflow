package filter

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEvent(t *testing.T, eventType string, data map[string]any) *entity.Event {
	t.Helper()
	payload, err := json.Marshal(data)
	require.NoError(t, err)
	return &entity.Event{
		ID:    entity.EventID("01HZTEST000000000000000000"),
		Type:  entity.EventType(eventType),
		Source: "test-service",
		Subject: "test:123",
		Data:  payload,
		Version: "v1",
		Metadata: map[string]string{},
	}
}

func TestShouldDeliver_EmptyExpression_DeliversAll(t *testing.T) {
	engine := NewRuleEngine()
	event := newTestEvent(t, "order.created", map[string]any{"amount": 50.0})

	ok, err := engine.ShouldDeliver(context.Background(), "", event)
	require.NoError(t, err)
	assert.True(t, ok, "empty expression should always deliver")
}

func TestShouldDeliver_MatchingExpression(t *testing.T) {
	engine := NewRuleEngine()
	event := newTestEvent(t, "order.created", map[string]any{"amount": 150.0, "currency": "USD"})

	ok, err := engine.ShouldDeliver(context.Background(), "event.type == 'order.created' && event.data.amount > 100", event)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestShouldDeliver_NonMatchingExpression(t *testing.T) {
	engine := NewRuleEngine()
	event := newTestEvent(t, "order.created", map[string]any{"amount": 50.0})

	ok, err := engine.ShouldDeliver(context.Background(), "event.type == 'order.created' && event.data.amount > 100", event)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestShouldDeliver_MismatchedEventType(t *testing.T) {
	engine := NewRuleEngine()
	event := newTestEvent(t, "order.canceled", map[string]any{"amount": 150.0})

	ok, err := engine.ShouldDeliver(context.Background(), "event.type == 'order.created'", event)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestShouldDeliver_SourceFilter(t *testing.T) {
	engine := NewRuleEngine()
	event := newTestEvent(t, "user.created", map[string]any{"email": "test@example.com"})

	ok, err := engine.ShouldDeliver(context.Background(), "event.source == 'billing-service'", event)
	require.NoError(t, err)
	assert.False(t, ok, "source is test-service, not billing-service")

	// Now test matching source
	ok, err = engine.ShouldDeliver(context.Background(), "event.source == 'test-service'", event)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestShouldDeliver_MetadataFilter(t *testing.T) {
	engine := NewRuleEngine()
	event := newTestEvent(t, "order.created", map[string]any{"amount": 200.0})
	event.Metadata["priority"] = "high"

	ok, err := engine.ShouldDeliver(context.Background(), "event.metadata['priority'] == 'high'", event)
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = engine.ShouldDeliver(context.Background(), "event.metadata['priority'] == 'low'", event)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestShouldDeliver_InvalidExpression_ReturnsError(t *testing.T) {
	engine := NewRuleEngine()
	event := newTestEvent(t, "order.created", map[string]any{"amount": 100.0})

	ok, err := engine.ShouldDeliver(context.Background(), "event.type == ", event)
	require.Error(t, err)
	assert.False(t, ok)
}

func TestShouldDeliver_NonBooleanResult_ReturnsError(t *testing.T) {
	engine := NewRuleEngine()
	event := newTestEvent(t, "order.created", map[string]any{"amount": 100.0})

	ok, err := engine.ShouldDeliver(context.Background(), "event.data.amount", event)
	require.Error(t, err)
	assert.False(t, ok)
}

func TestShouldDeliver_CachedProgramReuse(t *testing.T) {
	engine := NewRuleEngine()
	event := newTestEvent(t, "order.created", map[string]any{"amount": 150.0})

	expr := "event.type == 'order.created' && event.data.amount > 100"

	// First call compiles and caches.
	ok1, err := engine.ShouldDeliver(context.Background(), expr, event)
	require.NoError(t, err)
	assert.True(t, ok1)

	// Second call should reuse cached program (no error = cache hit).
	ok2, err := engine.ShouldDeliver(context.Background(), expr, event)
	require.NoError(t, err)
	assert.True(t, ok2)

	// Verify the program was cached.
	engine.mu.RLock()
	_, cached := engine.programs[expr]
	engine.mu.RUnlock()
	assert.True(t, cached, "compiled program should be cached")
}

func TestShouldDeliver_NilEventMetadata(t *testing.T) {
	engine := NewRuleEngine()
	payload, _ := json.Marshal(map[string]any{"amount": 100.0})
	event := &entity.Event{
		ID:      entity.EventID("01HZTEST000000000000000000"),
		Type:    "order.created",
		Source:  "test",
		Subject: "test:1",
		Data:    payload,
		Version: "v1",
	}

	// Should not panic on nil metadata.
	ok, err := engine.ShouldDeliver(context.Background(), "event.type == 'order.created'", event)
	require.NoError(t, err)
	assert.True(t, ok)
}
