package redis

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/alicebob/miniredis/v2"
	rd "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestLimiter(t *testing.T) (*ConcurrencyLimiter, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := rd.NewClient(&rd.Options{
		Addr: mr.Addr(),
	})

	limiter := NewConcurrencyLimiter(client, nil, "concurrency", 60*time.Second)
	return limiter, mr
}

func TestAcquire_WithinLimit(t *testing.T) {
	limiter, mr := newTestLimiter(t)
	defer mr.Close()

	ctx := context.Background()

	// Limit is 3 — first 3 acquisitions should succeed.
	ok, err := limiter.Acquire(ctx, "dest-001", 3)
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = limiter.Acquire(ctx, "dest-001", 3)
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = limiter.Acquire(ctx, "dest-001", 3)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestAcquire_ExceedsLimit(t *testing.T) {
	limiter, mr := newTestLimiter(t)
	defer mr.Close()

	ctx := context.Background()
	limit := 3

	// Acquire all 3 slots.
	for i := 0; i < limit; i++ {
		ok, err := limiter.Acquire(ctx, "dest-001", limit)
		require.NoError(t, err)
		assert.True(t, ok)
	}

	// 4th acquisition should fail.
	ok, err := limiter.Acquire(ctx, "dest-001", limit)
	require.NoError(t, err)
	assert.False(t, ok, "should reject when limit exceeded")
}

func TestRelease_DecrementsCounter(t *testing.T) {
	limiter, mr := newTestLimiter(t)
	defer mr.Close()

	ctx := context.Background()

	// Acquire at limit.
	for i := 0; i < 3; i++ {
		_, err := limiter.Acquire(ctx, "dest-001", 3)
		require.NoError(t, err)
	}

	// All slots full.
	ok, err := limiter.Acquire(ctx, "dest-001", 3)
	require.NoError(t, err)
	assert.False(t, ok)

	// Release one.
	err = limiter.Release(ctx, "dest-001")
	require.NoError(t, err)

	// Should be able to acquire again.
	ok, err = limiter.Acquire(ctx, "dest-001", 3)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestAcquire_ConcurrentAccess(t *testing.T) {
	limiter, mr := newTestLimiter(t)
	defer mr.Close()

	ctx := context.Background()
	limit := 5
	totalGoroutines := 20

	var acquired int32
	var wg sync.WaitGroup

	for i := 0; i < totalGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := limiter.Acquire(ctx, "dest-concurrent", limit)
			if err != nil {
				return
			}
			if ok {
				atomic.AddInt32(&acquired, 1)
				// Hold briefly, then release.
				time.Sleep(10 * time.Millisecond)
				_ = limiter.Release(ctx, "dest-concurrent")
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int32(limit), atomic.LoadInt32(&acquired),
		"exactly limit goroutines should have acquired a slot")
}

func TestAcquire_SeparateKeys(t *testing.T) {
	limiter, mr := newTestLimiter(t)
	defer mr.Close()

	ctx := context.Background()

	// Key A and Key B are independent — both should reach their full limit.
	for i := 0; i < 3; i++ {
		ok, err := limiter.Acquire(ctx, "dest-A", 3)
		require.NoError(t, err)
		assert.True(t, ok)
	}
	for i := 0; i < 3; i++ {
		ok, err := limiter.Acquire(ctx, "dest-B", 3)
		require.NoError(t, err)
		assert.True(t, ok)
	}

	// Both keys should be at their limit.
	okA, err := limiter.Acquire(ctx, "dest-A", 3)
	require.NoError(t, err)
	assert.False(t, okA)

	okB, err := limiter.Acquire(ctx, "dest-B", 3)
	require.NoError(t, err)
	assert.False(t, okB)
}

func TestAcquire_DefaultTTL(t *testing.T) {
	limiter, mr := newTestLimiter(t)
	defer mr.Close()

	ctx := context.Background()

	_, err := limiter.Acquire(ctx, "dest-ttl", 1)
	require.NoError(t, err)

	// The key should have a TTL set.
	key := limiter.key("dest-ttl")
	ttl := mr.TTL(key)
	assert.True(t, ttl > 0, "key should have a TTL")
	assert.LessOrEqual(t, int(ttl.Seconds()), 60, "TTL should be at most 60 seconds")
}

func TestRelease_EmptyKey(t *testing.T) {
	limiter, mr := newTestLimiter(t)
	defer mr.Close()

	ctx := context.Background()

	// Release on a non-existent key should not error.
	err := limiter.Release(ctx, "dest-nonexistent")
	require.NoError(t, err)
}

func TestConcurrencyLimiter_ImplementsInterface(t *testing.T) {
	limiter, mr := newTestLimiter(t)
	defer mr.Close()

	var _ entity.ConcurrencyLimiter = limiter
}
