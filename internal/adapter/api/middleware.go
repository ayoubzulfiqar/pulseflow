package api

import (
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/config"
	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"golang.org/x/time/rate"
)

// --- Request ID middleware ---

// RequestID generates or propagates an X-Request-ID header and stores it
// in Fiber locals for downstream correlation.
func RequestID() fiber.Handler {
	return func(c *fiber.Ctx) error {
		rid := c.Get("X-Request-ID")
		if rid == "" {
			rid = uuid.NewString()
		}
		c.Locals("request_id", rid)
		c.Set("X-Request-ID", rid)
		return c.Next()
	}
}

func getLocalRequestID(c *fiber.Ctx) string {
	if v, ok := c.Locals("request_id").(string); ok {
		return v
	}
	return "-"
}

// --- Structured logging middleware ---

// Logger logs each request with method, path, status, latency, and request ID.
func Logger(logger *slog.Logger) fiber.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()

		logger.Info("request",
			"method", c.Method(),
			"path", c.Path(),
			"status", c.Response().StatusCode(),
			"size", len(c.Response().Body()),
			"duration_ms", time.Since(start).Milliseconds(),
			"request_id", getLocalRequestID(c),
			"ip", realIP(c),
			"user_agent", c.Get("User-Agent"),
		)
		return err
	}
}

// --- Panic recovery middleware ---

// Recover catches panics in downstream handlers and returns a 500 JSON error.
func Recover(logger *slog.Logger) fiber.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(c *fiber.Ctx) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered",
					"panic", r, "path", c.Path(), "method", c.Method())
				err = c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "internal server error",
				})
			}
		}()
		return c.Next()
	}
}

// --- Rate limiter ---

type rlEntry struct {
	limiter  *rate.Limiter
	lastSeen atomic.Int64 // unix nanoseconds
}

// RateLimiter implements per-IP token-bucket rate limiting with
// periodic cleanup of stale entries.
type RateLimiter struct {
	entries map[string]*rlEntry
	mu      sync.RWMutex
	rps     rate.Limit
	burst   int
	ttl     time.Duration
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewRateLimiter creates a RateLimiter from config.
func NewRateLimiter(cfg *config.RateLimitConfig) *RateLimiter {
	rl := &RateLimiter{
		entries: make(map[string]*rlEntry),
		rps:     rate.Limit(cfg.DefaultRPS),
		burst:   int(float64(cfg.BurstMultiplier) * cfg.DefaultRPS),
		ttl:     5 * time.Minute,
		stopCh:  make(chan struct{}),
	}
	rl.wg.Add(1)
	go rl.cleanupLoop()
	return rl
}

// Handler returns the Fiber middleware function.
func (rl *RateLimiter) Handler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		ip := realIP(c)
		if !rl.getLimiter(ip).Allow() {
			retryAfter := strconv.Itoa(int(1.0 / float64(rl.rps)))
			c.Set("Retry-After", retryAfter)
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error":       "rate limit exceeded",
				"retry_after": retryAfter,
			})
		}
		return c.Next()
	}
}

// Stop signals the background cleanup goroutine to exit and waits for it.
func (rl *RateLimiter) Stop() {
	close(rl.stopCh)
	rl.wg.Wait()
}

func (rl *RateLimiter) getLimiter(key string) *rate.Limiter {
	now := time.Now().UnixNano()

	rl.mu.RLock()
	if e, ok := rl.entries[key]; ok {
		e.lastSeen.Store(now)
		rl.mu.RUnlock()
		return e.limiter
	}
	rl.mu.RUnlock()

	rl.mu.Lock()
	defer rl.mu.Unlock()
	// Double-check after acquiring write lock.
	if e, ok := rl.entries[key]; ok {
		e.lastSeen.Store(now)
		return e.limiter
	}
	e := &rlEntry{
		limiter: rate.NewLimiter(rl.rps, rl.burst),
	}
	e.lastSeen.Store(now)
	rl.entries[key] = e
	return e.limiter
}

func (rl *RateLimiter) cleanup() {
	cutoff := time.Now().Add(-rl.ttl).UnixNano()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for k, v := range rl.entries {
		if v.lastSeen.Load() < cutoff {
			delete(rl.entries, k)
		}
	}
}

func (rl *RateLimiter) cleanupLoop() {
	defer rl.wg.Done()
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rl.cleanup()
		case <-rl.stopCh:
			return
		}
	}
}

// realIP extracts the client IP, respecting X-Forwarded-For.
func realIP(c *fiber.Ctx) string {
	if fwd := c.Get("X-Forwarded-For"); fwd != "" {
		if idx := strings.Index(fwd, ","); idx > 0 {
			return strings.TrimSpace(fwd[:idx])
		}
		return strings.TrimSpace(fwd)
	}
	return c.IP()
}
