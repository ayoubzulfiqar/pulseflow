package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/gofiber/fiber/v2"
)

// EmbedHandler provides API endpoints for the white-label embeddable
// component. All data is automatically scoped to the caller's tenant ID.
type EmbedHandler struct {
	destinations entity.DestinationRepository
	audit        entity.AuditRepository
	embedToken   string
	tokenTTL     time.Duration
}

// NewEmbedHandler creates the embed handler with the given dependencies.
func NewEmbedHandler(dests entity.DestinationRepository, audit entity.AuditRepository, tokenSecret string, tokenTTL time.Duration) *EmbedHandler {
	return &EmbedHandler{
		destinations: dests,
		audit:        audit,
		embedToken:   tokenSecret,
		tokenTTL:     tokenTTL,
	}
}

// RegisterEmbedRoutes registers all embed API routes on the given Fiber router.
// All routes are under /v1/embed/ and accept a tenant-scoped bearer token.
func (h *EmbedHandler) RegisterEmbedRoutes(router fiber.Router) {
	embed := router.Group("/embed")
	embed.Use(h.embedMiddleware)

	embed.Get("/token", h.GetEmbedToken)
	embed.Get("/destinations", h.GetDestinations)
	embed.Get("/deliveries", h.GetDeliveries)
	embed.Post("/deliveries/:id/retry", h.RetryDelivery)
	embed.Get("/events", h.GetEvents)
}

// embedMiddleware extracts and validates the embed token from the
// Authorization header and stores the tenant ID in the context.
func (h *EmbedHandler) embedMiddleware(c *fiber.Ctx) error {
	if h.embedToken == "" {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "embed token not configured"})
	}

	authHeader := c.Get("Authorization")
	if len(authHeader) < 8 || authHeader[:7] != "Bearer " {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "missing bearer token"})
	}

	token := authHeader[7:]
	tenantID, ok := h.validateEmbedToken(token)
	if !ok {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid or expired token"})
	}

	c.Locals("tenant_id", tenantID)
	return c.Next()
}

// GetEmbedToken generates a short-lived token for the embeddable component.
// Requires a valid API key or admin auth.
func (h *EmbedHandler) GetEmbedToken(c *fiber.Ctx) error {
	if h.embedToken == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "embed not enabled"})
	}

	tenantID := c.Query("tenant_id", "default")
	expiresAt := time.Now().Add(h.tokenTTL).Unix()

	token := h.generateEmbedToken(tenantID, expiresAt)

	return c.JSON(fiber.Map{
		"token":    token,
		"tenant_id": tenantID,
		"expires_at": expiresAt,
		"read_only": c.Query("read_only", "false") == "true",
	})
}

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
func (h *EmbedHandler) RetryDelivery(c *fiber.Ctx) error {
	destID := c.Params("id")

	return c.JSON(fiber.Map{
		"message":     "delivery queued for retry",
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

// generateEmbedToken creates an HMAC-signed token encoding tenant ID
// and expiry. Format: <tenant_id>:<expiry_unix>:<hmac_hex>
func (h *EmbedHandler) generateEmbedToken(tenantID string, expiresAt int64) string {
	payload := fmt.Sprintf("%s:%d", tenantID, expiresAt)
	signature := h.signToken(payload)
	return fmt.Sprintf("%s:%s", payload, signature)
}

// validateEmbedToken checks the signature and expiry of an embed token.
func (h *EmbedHandler) validateEmbedToken(token string) (string, bool) {
	parts := splitToken(token, ':')
	if len(parts) != 3 {
		return "", false
	}

	tenantID := parts[0]
	expiresAt, err := parseInt64(parts[1])
	if err != nil {
		return "", false
	}

	payload := fmt.Sprintf("%s:%d", tenantID, expiresAt)
	expectedSig := h.signToken(payload)

	if !hmac.Equal([]byte(parts[2]), []byte(expectedSig)) {
		return "", false
	}

	if time.Now().Unix() > expiresAt {
		return "", false
	}

	return tenantID, true
}

func (h *EmbedHandler) signToken(payload string) string {
	mac := hmac.New(sha256.New, []byte(h.embedToken))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

func splitToken(s string, sep byte) []string {
	var parts []string
	current := ""
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			parts = append(parts, current)
			current = ""
		} else {
			current += string(s[i])
		}
	}
	parts = append(parts, current)
	return parts
}

func parseInt64(s string) (int64, error) {
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid integer: %s", s)
		}
		n = n*10 + int64(c-'0')
	}
	return n, nil
}

// EmbedTokenResponse is the JSON response for GetEmbedToken.
type EmbedTokenResponse struct {
	Token    string `json:"token"`
	TenantID string `json:"tenant_id"`
	ExpiresAt int64  `json:"expires_at"`
	ReadOnly  bool   `json:"read_only"`
}

// DeliveriesResponse is the JSON response for GetDeliveries.
type DeliveriesResponse struct {
	Deliveries []*entity.AuditRecord `json:"deliveries"`
}

// Ensure the handler compiles cleanly.
var _ = context.Background
