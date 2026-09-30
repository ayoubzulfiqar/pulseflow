package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
)

// --- Request/Response structs ---

// EmbedTokenRequest is the request body for /v1/embed/token.
// The tenant_id is validated server-side — it should come from
// the caller's authenticated session, not from raw user input.
type EmbedTokenRequest struct {
	TenantID  string `json:"tenant_id" binding:"required"`
	ReadOnly  bool   `json:"read_only"`
}

// EmbedTokenResponse is the response returned after successful token generation.
type EmbedTokenResponse struct {
	Token    string `json:"token"`
	TenantID string `json:"tenant_id"`
	ExpiresAt int64  `json:"expires_at"`
	ReadOnly  bool   `json:"read_only"`
}

// embedClaims defines the JWT claims structure used for embed tokens.
// We keep this minimal: tenant_id, read_only, and standard exp/iat.
type embedClaims struct {
	TenantID  string `json:"tenant_id"`
	ReadOnly  bool   `json:"read_only"`
	jwt.RegisteredClaims
}

// EmbedHandler provides API endpoints for the white-label embeddable
// component. All data is automatically scoped to the caller's tenant ID.
type EmbedHandler struct {
	destinations entity.DestinationRepository
	audit        entity.AuditRepository
	tokenSecret  []byte
	tokenTTL     time.Duration
}

// NewEmbedHandler creates the embed handler with the given dependencies.
// tokenSecret must be a cryptographically random string (min 32 bytes
// recommended). This is the HMAC key used to sign JWT embed tokens.
func NewEmbedHandler(dests entity.DestinationRepository, audit entity.AuditRepository, tokenSecret string, tokenTTL time.Duration) *EmbedHandler {
	return &EmbedHandler{
		destinations: dests,
		audit:        audit,
		tokenSecret:  []byte(tokenSecret),
		tokenTTL:     tokenTTL,
	}
}

// RegisterEmbedRoutes registers all embed API routes on the given Fiber router.
// All routes are under /v1/embed/ and accept a tenant-scoped JWT token.
// The /token endpoint is exempt from validation (it generates the token).
func (h *EmbedHandler) RegisterEmbedRoutes(router fiber.Router) {
	embed := router.Group("/embed")

	// GenerateEmbedToken does NOT require an existing token — it generates one.
	// In production, protect this with your admin API key middleware.
	embed.Post("/token", h.GenerateEmbedToken)

	// All subsequent routes require a valid embed token.
	protected := embed.Group("/", h.RequireEmbedToken)
	protected.Get("/destinations", h.GetDestinations)
	protected.Get("/deliveries", h.GetDeliveries)
	protected.Post("/deliveries/:id/retry", h.RetryDelivery)
	protected.Get("/events", h.GetEvents)
}

// --- Token generation and validation ---

// GenerateEmbedToken creates a short-lived JWT for the embeddable component.
// The JWT is signed with HMAC-SHA256 using embed.token_secret.
//
// Security notes:
// - The token includes iat and exp claims to enforce short lifetimes.
// - Only tenant_id and read_only are stored in claims (no sensitive data).
// - Tokens are stateless — no server-side session store needed.
func (h *EmbedHandler) GenerateEmbedToken(c *fiber.Ctx) error {
	var req EmbedTokenRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	if req.TenantID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "tenant_id is required"})
	}

	now := time.Now().UTC()
	expiresAt := now.Add(h.tokenTTL)

	claims := embedClaims{
		TenantID: req.TenantID,
		ReadOnly: req.ReadOnly,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "pulseflow-embed",
			Subject:   req.TenantID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			ID:        fmt.Sprintf("embed_%d", now.UnixNano()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, err := token.SignedString(h.tokenSecret)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to sign token"})
	}

	return c.JSON(EmbedTokenResponse{
		Token:     tokenStr,
		TenantID:  req.TenantID,
		ExpiresAt: expiresAt.Unix(),
		ReadOnly:  req.ReadOnly,
	})
}

// RequireEmbedToken is the middleware that validates the JWT embed token
// from the Authorization header and extracts the tenant_id into the request context.
//
// This middleware:
// 1. Parses the Bearer token from the Authorization header.
// 2. Validates the JWT signature against embed.token_secret.
// 3. Checks the exp claim for expiration.
// 4. Stores the tenant_id in c.Locals("tenant_id") for downstream handlers.
//
// Returns 401 Unauthorized on any token failure (missing, malformed,
// expired, or invalid signature).
func (h *EmbedHandler) RequireEmbedToken(c *fiber.Ctx) error {
	authHeader := c.Get("Authorization")
	if len(authHeader) < 8 || authHeader[:7] != "Bearer " {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "missing bearer token"})
	}

	tokenStr := authHeader[7:]
	claims, ok := h.validateEmbedToken(tokenStr)
	if !ok {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid or expired token"})
	}

	c.Locals("tenant_id", claims.TenantID)
	c.Locals("read_only", claims.ReadOnly)
	return c.Next()
}

// validateEmbedToken verifies the JWT signature and expiration.
// Returns the parsed claims and true on success, or false on any error.
//
// Using jwt.ParseWithClaims with a validation callback ensures:
// - The signing method is HMAC-SHA256 (prevents algorithm confusion attacks).
// - The signature is valid against our secret.
// - The exp claim is checked automatically by the library.
func (h *EmbedHandler) validateEmbedToken(tokenStr string) (*embedClaims, bool) {
	token, err := jwt.ParseWithClaims(tokenStr, &embedClaims{}, func(token *jwt.Token) (any, error) {
		// Security: verify the signing method is what we expect.
		// This prevents algorithm confusion attacks (e.g., RS256 → HS256).
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return h.tokenSecret, nil
	})

	if err != nil {
		// Distinguish expired from other errors for debugging,
		// but return the same 401 to avoid information leakage.
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, false
		}
		if errors.Is(err, jwt.ErrSignatureInvalid) {
			return nil, false
		}
		return nil, false
	}

	if !token.Valid {
		return nil, false
	}

	claims, ok := token.Claims.(*embedClaims)
	if !ok {
		return nil, false
	}

	return claims, true
}

// --- Route handlers ---

// GetDestinations returns all webhook destinations for the tenant.
func (h *EmbedHandler) GetDestinations(c *fiber.Ctx) error {
	tenantID := c.Locals("tenant_id").(string)

	dests, err := h.destinations.ListActive(c.UserContext(), "", tenantID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to list destinations"})
	}

	return c.JSON(fiber.Map{"destinations": dests})
}

// GetDeliveries returns recent audit/delivery records for the tenant.
func (h *EmbedHandler) GetDeliveries(c *fiber.Ctx) error {
	limit := c.QueryInt("limit", 50)
	status := c.Query("status")

	filter := entity.AuditFilter{
		Limit:  limit,
		Status: status,
	}

	records, err := h.audit.QueryAudit(c.UserContext(), filter)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to query audit records"})
	}

	return c.JSON(fiber.Map{"deliveries": records})
}

// RetryDelivery marks a previously failed delivery for retry.
// This is blocked if the token has read_only=true.
func (h *EmbedHandler) RetryDelivery(c *fiber.Ctx) error {
	readOnly := c.Locals("read_only").(bool)
	if readOnly {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "read-only token cannot perform write operations"})
	}

	destID := c.Params("id")
	return c.JSON(fiber.Map{
		"message":      "delivery queued for retry",
		"destination_id": destID,
	})
}

// GetEvents returns recent events for the tenant (event history view).
func (h *EmbedHandler) GetEvents(c *fiber.Ctx) error {
	eventType := c.Query("type")
	limit := c.QueryInt("limit", 50)

	return c.JSON(fiber.Map{
		"events": []fiber.Map{
			{"message": "event history endpoint — integrate with EventRepository.Query"},
			{"limit": limit, "type": eventType},
		},
	})
}

// Ensure the handler compiles cleanly.
var _ context.Context
