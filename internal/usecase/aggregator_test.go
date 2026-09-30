package usecase

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDeliverer records all delivery calls for testing.
type mockDeliverer struct {
	mu         sync.Mutex
	deliveries []deliveryRecord
}

type deliveryRecord struct {
	dest  *entity.Destination
	event *entity.Event
}

func (m *mockDeliverer) Deliver(ctx context.Context, dest *entity.Destination, event *entity.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deliveries = append(m.deliveries, deliveryRecord{dest: dest, event: event})
}

func (m *mockDeliverer) DeliveryCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.deliveries)
}

func newBatchEvent(data map[string]any) *entity.Event {
	payload, _ := json.Marshal(data)
	return &entity.Event{
		ID:      entity.EventID("01HZTEST000000000000000000"),
		Type:    "cart.item_added",
		Source:  "ecom",
		Subject: "cart:123",
		Data:    payload,
		Version: "v1",
	}
}

func TestBatchAggregator_EnqueueAndPending(t *testing.T) {
	deliverer := &mockDeliverer{}
	ba := NewBatchAggregator(deliverer, 1*time.Second, nil)

	rule := &entity.BatchingRule{
		Enabled:         true,
		MaxBatchSize:    10,
		MaxWaitDuration: 30 * time.Second,
		EventPattern:    "cart.item_added",
	}

	event := newBatchEvent(map[string]any{"item": "widget", "qty": 1})
	err := ba.Enqueue(context.Background(), "dest_1", event, rule)
	require.NoError(t, err)

	count, err := ba.PendingCount(context.Background(), "dest_1")
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestBatchAggregator_Flush(t *testing.T) {
	deliverer := &mockDeliverer{}
	ba := NewBatchAggregator(deliverer, 1*time.Second, nil)

	rule := &entity.BatchingRule{
		Enabled:         true,
		MaxBatchSize:    10,
		MaxWaitDuration: 30 * time.Second,
	}

	// Enqueue 3 events.
	for i := 0; i < 3; i++ {
		event := newBatchEvent(map[string]any{"item": "widget", "qty": i + 1})
		err := ba.Enqueue(context.Background(), "dest_1", event, rule)
		require.NoError(t, err)
	}

	// Flush and verify.
	events, err := ba.Flush(context.Background(), "dest_1")
	require.NoError(t, err)
	require.Len(t, events, 3)

	// Pending count should be 0 after flush.
	count, err := ba.PendingCount(context.Background(), "dest_1")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestBatchAggregator_NoBatchingRule(t *testing.T) {
	deliverer := &mockDeliverer{}
	ba := NewBatchAggregator(deliverer, 1*time.Second, nil)

	rule := &entity.BatchingRule{Enabled: false}

	event := newBatchEvent(map[string]any{"item": "widget"})
	err := ba.Enqueue(context.Background(), "dest_1", event, rule)
	require.NoError(t, err)

	// With batching disabled, the event is delivered immediately.
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, deliverer.DeliveryCount())

	// No events should be buffered.
	count, err := ba.PendingCount(context.Background(), "dest_1")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestBatchAggregator_FlushEmptyBuffer(t *testing.T) {
	deliverer := &mockDeliverer{}
	ba := NewBatchAggregator(deliverer, 1*time.Second, nil)

	events, err := ba.Flush(context.Background(), "dest_nonexistent")
	require.NoError(t, err)
	assert.Nil(t, events)
}

func TestBatchAggregator_ConcurrentEnqueue(t *testing.T) {
	deliverer := &mockDeliverer{}
	ba := NewBatchAggregator(deliverer, 1*time.Second, nil)

	rule := &entity.BatchingRule{
		Enabled:         true,
		MaxBatchSize:    100,
		MaxWaitDuration: 60 * time.Second,
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			event := newBatchEvent(map[string]any{"item": "widget", "index": i})
			_ = ba.Enqueue(context.Background(), "dest_concurrent", event, rule)
		}(i)
	}
	wg.Wait()

	count, err := ba.PendingCount(context.Background(), "dest_concurrent")
	require.NoError(t, err)
	assert.Equal(t, 10, count)
}

func TestBatchAggregator_ImplementsInterface(t *testing.T) {
	var _ entity.Batcher = (*BatchAggregator)(nil)
}
