package redis

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/alicebob/miniredis/v2"
	rd "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *rd.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := rd.NewClient(&rd.Options{
		Addr: mr.Addr(),
	})

	return mr, client
}

func testEvent() *entity.Event {
	return &entity.Event{
		ID:      entity.EventID("01HZTEST000000000000000001"),
		Type:    "order.created",
		Source:  "billing",
		Subject: "order:1",
		Data:    json.RawMessage(`{"amount": 100.0}`),
		Version: "v1",
	}
}

func TestDeduplicator_CheckAndMark_NewEvent(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	defer client.Close()

	d := NewDeduplicator(client)
	event := testEvent()
	key, err := d.GenerateKey(event, "")
	require.NoError(t, err)

	isDup, err := d.CheckAndMark(context.Background(), key, 5*time.Minute)
	require.NoError(t, err)
	assert.False(t, isDup, "first event should not be a duplicate")
}

func TestDeduplicator_CheckAndMark_DuplicateEvent(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	defer client.Close()

	d := NewDeduplicator(client)
	event := testEvent()
	key, err := d.GenerateKey(event, "")
	require.NoError(t, err)

	// First mark should be new.
	_, err = d.CheckAndMark(context.Background(), key, 5*time.Minute)
	require.NoError(t, err)

	// Second mark should be duplicate.
	isDup, err := d.CheckAndMark(context.Background(), key, 5*time.Minute)
	require.NoError(t, err)
	assert.True(t, isDup, "second event should be a duplicate")
}

func TestDeduplicator_CheckAndMark_ExpiredEvent(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	defer client.Close()

	d := NewDeduplicator(client)
	event := testEvent()
	key, err := d.GenerateKey(event, "")
	require.NoError(t, err)

	// Mark with short window.
	_, err = d.CheckAndMark(context.Background(), key, 50*time.Millisecond)
	require.NoError(t, err)

	// Wait for expiry.
	time.Sleep(100 * time.Millisecond)

	// Advance miniredis time.
	mr.FastForward(100 * time.Millisecond)

	// Should be treated as new after expiry.
	isDup, err := d.CheckAndMark(context.Background(), key, 50*time.Millisecond)
	require.NoError(t, err)
	assert.False(t, isDup, "expired event should not be duplicate")
}

func TestDeduplicator_Seen(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	defer client.Close()

	d := NewDeduplicator(client)
	event := testEvent()
	key, err := d.GenerateKey(event, "")
	require.NoError(t, err)

	// Should not be seen initially.
	seen, err := d.Seen(context.Background(), key, time.Minute)
	require.NoError(t, err)
	assert.False(t, seen)

	// Mark it.
	_, err = d.CheckAndMark(context.Background(), key, time.Minute)
	require.NoError(t, err)

	// Should be seen now.
	seen, err = d.Seen(context.Background(), key, time.Minute)
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestDeduplicator_GenerateKey_DifferentEvents(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	defer client.Close()

	d := NewDeduplicator(client)

	event1 := testEvent()
	event2 := testEvent()
	event2.Data = json.RawMessage(`{"amount": 200.0}`)

	key1, err := d.GenerateKey(event1, "")
	require.NoError(t, err)
	key2, err := d.GenerateKey(event2, "")
	require.NoError(t, err)

	assert.NotEqual(t, key1, key2, "different payloads should produce different keys")
}

func TestDeduplicator_GenerateKey_IdempotencyKey(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	defer client.Close()

	d := NewDeduplicator(client)
	event := testEvent()

	key1, err := d.GenerateKey(event, "idempotency-123")
	require.NoError(t, err)
	key2, err := d.GenerateKey(event, "idempotency-456")
	require.NoError(t, err)

	assert.NotEqual(t, key1, key2, "different idempotency keys should produce different dedup keys")
}

func TestDeduplicator_GenerateKey_SameEventSameKey(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	defer client.Close()

	d := NewDeduplicator(client)
	event := testEvent()

	key1, err := d.GenerateKey(event, "")
	require.NoError(t, err)
	key2, err := d.GenerateKey(event, "")
	require.NoError(t, err)

	assert.Equal(t, key1, key2, "same event should produce same key")
}

func TestDeduplicator_Mark(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	defer client.Close()

	d := NewDeduplicator(client)
	event := testEvent()
	key, err := d.GenerateKey(event, "")
	require.NoError(t, err)

	err = d.Mark(context.Background(), key)
	require.NoError(t, err)

	// Key should exist now.
	seen, err := d.Seen(context.Background(), key, time.Hour)
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestDeduplicator_ConcurrentAccess(t *testing.T) {
	mr, client := newTestRedis(t)
	defer mr.Close()
	defer client.Close()

	d := NewDeduplicator(client)
	event := testEvent()
	key, err := d.GenerateKey(event, "")
	require.NoError(t, err)

	// Concurrent check-and-mark should be idempotent.
	var successCount int32
	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func() {
			isNew, err := d.CheckAndMark(context.Background(), key, 5*time.Minute)
			require.NoError(t, err)
			if !isNew {
				atomic.AddInt32(&successCount, 1)
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	// Exactly 1 should be new, 9 should be duplicates.
	assert.Equal(t, int32(1), atomic.LoadInt32(&successCount))
}

func TestDeduplicator_ImplementsInterface(t *testing.T) {
	var _ entity.Deduplicator = (*RedisDeduplicator)(nil)
}
