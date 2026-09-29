package api

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/config"
	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/gofiber/fiber/v2"
	"golang.org/x/time/rate"
)

// TenantContextKey is the context key under which the authenticated tenant
// ID is stored. Downstream handlers and middleware can extract it via
// TenantFromContext.
type contextKey string

const TenantContextKey contextKey = "tenant_id"

// TenantFromContext extracts the authenticated tenant ID from the context.
func TenantFromContext(c *fiber.Ctx) string {
	if v, ok := c.Locals(TenantContextKey).(string); ok {
		return v
	}
	return ""
}

// AdminAuthConfig controls whether API key auth is required.
type AdminAuthConfig struct {
	Enabled bool
}

// APIKeyAuth is the Fiber middleware that validates the X-API-Key header
// against the tenant repository and attaches the tenant to the request context.
// Requests without a valid key are rejected with 401.
func APIKeyAuth(repo entity.TenantRepository, logger *slog.Logger) fiber.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(c *fiber.Ctx) error {
		key := c.Get("X-API-Key")
		if key == "" {
			// Allow requests without a key if auth is not enforced (e.g. local dev).
			if repo == nil {
				return c.Next()
			}
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "x-api-key header required",
			})
		}

		if repo == nil {
			// No repository configured — allow all keys (dev mode).
			return c.Next()
		}

		ctx, cancel := makeRequestContext(c)
		defer cancel()

		tenant, err := repo.ValidateAPIKey(ctx, key)
		if err != nil {
			logger.Warn("auth: invalid api key", "path", c.Path(), "error", err)
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "invalid or expired api key",
			})
		}

		c.Locals(TenantContextKey, tenant.ID)
		c.Locals("tenant", tenant)
		return c.Next()
	}
}

// makeRequestContext creates a short-lived context with a timeout for
// repository lookups.
func makeRequestContext(c *fiber.Ctx) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.UserContext(), 5*time.Second)
}

// PerTenantRateLimiter implements token-bucket rate limiting scoped per tenant.
// When tenant auth is disabled, it falls back to per-IP limiting (same as
// the original RateLimiter).
type PerTenantRateLimiter struct {
	entries map[string]*rlEntry
	mu      sync.RWMutex
	rps     rate.Limit
	burst   int
	ttl     time.Duration
	stopCh  chan struct{}
	wg      sync.WaitGroup
	logger  *slog.Logger
}

// NewPerTenantRateLimiter creates a per-tenant rate limiter.
func NewPerTenantRateLimiter(cfg *config.RateLimitConfig, logger *slog.Logger) *PerTenantRateLimiter {
	if logger == nil {
		logger = slog.Default()
	}
	rl := &PerTenantRateLimiter{
		entries: make(map[string]*rlEntry),
		rps:     rate.Limit(cfg.DefaultRPS),
		burst:   int(float64(cfg.BurstMultiplier) * cfg.DefaultRPS),
		ttl:     5 * time.Minute,
		stopCh:  make(chan struct{}),
		logger:  logger,
	}
	rl.wg.Add(1)
	go rl.cleanupLoop()
	return rl
}

// Handler returns the Fiber middleware function. It uses the tenant ID from
// context if available, otherwise falls back to the client IP.
func (rl *PerTenantRateLimiter) Handler() fiber.Handler {
	return func(c *fiber.Ctx) error {
		tenantID := TenantFromContext(c)
		var key string
		if tenantID != "" {
			key = "tenant:" + tenantID
		} else {
			key = "ip:" + realIP(c)
		}

		if !rl.getLimiter(key).Allow() {
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
func (rl *PerTenantRateLimiter) Stop() {
	close(rl.stopCh)
	rl.wg.Wait()
}

func (rl *PerTenantRateLimiter) getLimiter(key string) *rate.Limiter {
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

func (rl *PerTenantRateLimiter) cleanup() {
	cutoff := time.Now().Add(-rl.ttl).UnixNano()
	rl.mu.Lock()
	defer rl.mu.Unlock()
	for k, v := range rl.entries {
		if v.lastSeen.Load() < cutoff {
			delete(rl.entries, k)
		}
	}
}

func (rl *PerTenantRateLimiter) cleanupLoop() {
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

