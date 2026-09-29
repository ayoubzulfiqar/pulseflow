package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	rd "github.com/redis/go-redis/v9"
)

// ConcurrencyLimiter implements entity.ConcurrencyLimiter using Redis
// as a distributed counting semaphore. Each destination gets its own key
// with a TTL, and the count is managed atomically via a Lua script to
// avoid race conditions under concurrent webhook workers.
type ConcurrencyLimiter struct {
	client rd.Cmdable
	logger *slog.Logger
	prefix string
	ttl    time.Duration
}

// NewConcurrencyLimiter creates a Redis-backed concurrency limiter.
// The prefix is used to namespace Redis keys (e.g. "concurrency:dest_id").
// TTL ensures stale locks from crashed workers expire automatically.
func NewConcurrencyLimiter(client rd.Cmdable, logger *slog.Logger, prefix string, ttl time.Duration) *ConcurrencyLimiter {
	if logger == nil {
		logger = slog.Default()
	}
	if prefix == "" {
		prefix = "concurrency"
	}
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return &ConcurrencyLimiter{
		client: client,
		logger: logger,
		prefix: prefix,
		ttl:    ttl,
	}
}

// Acquire atomically increments the counter for the given key. If the new
// value exceeds the limit, the increment is rolled back and false is returned.
// The caller MUST call Release when done.
func (l *ConcurrencyLimiter) Acquire(ctx context.Context, key string, limit int) (bool, error) {
	result, err := l.client.Eval(ctx, acquireScript, []string{l.key(key)}, limit, int64(l.ttl.Seconds())).Result()
	if err != nil {
		return false, fmt.Errorf("concurrency: acquire: %w", err)
	}

	allowed, ok := result.(string)
	if !ok {
		return false, fmt.Errorf("concurrency: unexpected response type: %T", result)
	}

	return allowed == "true", nil
}

// Release atomically decrements the counter for the given key.
// If the counter reaches zero, the key is deleted to free Redis memory.
func (l *ConcurrencyLimiter) Release(ctx context.Context, key string) error {
	_, err := l.client.Eval(ctx, releaseScript, []string{l.key(key)}).Result()
	if err != nil {
		return fmt.Errorf("concurrency: release: %w", err)
	}
	return nil
}

// key builds the Redis key for a destination.
func (l *ConcurrencyLimiter) key(destID string) string {
	return fmt.Sprintf("%s:%s", l.prefix, destID)
}

// acquireScript atomically increments the counter and checks against the limit.
// If the count exceeds the limit, the increment is rolled back via DECR.
const acquireScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local current = redis.call("INCR", key)
if current == 1 then
	redis.call("EXPIRE", key, ttl)
end
if current > limit then
	redis.call("DECR", key)
	return "false"
end
return "true"
`

// releaseScript atomically decrements the counter and deletes the key if zero.
const releaseScript = `
local key = KEYS[1]
local current = redis.call("DECR", key)
if current <= 0 then
	redis.call("DEL", key)
end
return current
`

// Ensure ConcurrencyLimiter satisfies entity.ConcurrencyLimiter.
var _ entity.ConcurrencyLimiter = (*ConcurrencyLimiter)(nil)
