package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler() *EmbedHandler {
	return NewEmbedHandler(nil, nil, "test-secret-key", 24*time.Hour)
}

func newTestApp(h *EmbedHandler) *fiber.App {
	app := fiber.New()
	app.Post("/token", h.GenerateEmbedToken)
	app.Get("/test", h.RequireEmbedToken, func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{"tenant": c.Locals("tenant_id")})
	})
	app.Post("/test/:id/retry", h.RequireEmbedToken, h.RetryDelivery)
	return app
}

func doRequest(app *fiber.App, method, path string, body string, token string) *http.Response {
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, _ := app.Test(req)
	return resp
}

func getToken(t *testing.T, app *fiber.App, reqBody string) string {
	resp := doRequest(app, "POST", "/token", reqBody, "")
	defer resp.Body.Close()
	var body EmbedTokenResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	return body.Token
}

func TestGenerateEmbedToken_CreatesValidJWT(t *testing.T) {
	h := newTestHandler()
	app := newTestApp(h)

	resp := doRequest(app, "POST", "/token", `{"tenant_id":"tenant_8f9a2b","read_only":false}`, "")
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body EmbedTokenResponse
	err := json.NewDecoder(resp.Body).Decode(&body)
	require.NoError(t, err)

	assert.NotEmpty(t, body.Token)
	assert.Equal(t, "tenant_8f9a2b", body.TenantID)
	assert.False(t, body.ReadOnly)
	assert.True(t, body.ExpiresAt > time.Now().Unix())
}

func TestGenerateEmbedToken_ReadOnly(t *testing.T) {
	h := newTestHandler()
	app := newTestApp(h)

	resp := doRequest(app, "POST", "/token", `{"tenant_id":"tenant_abc","read_only":true}`, "")
	defer resp.Body.Close()

	var body EmbedTokenResponse
	json.NewDecoder(resp.Body).Decode(&body)
	assert.True(t, body.ReadOnly)
}

func TestGenerateEmbedToken_MissingTenantID(t *testing.T) {
	h := newTestHandler()
	app := newTestApp(h)

	resp := doRequest(app, "POST", "/token", `{"read_only":false}`, "")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGenerateEmbedToken_InvalidJSON(t *testing.T) {
	h := newTestHandler()
	app := newTestApp(h)

	resp := doRequest(app, "POST", "/token", `not json`, "")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestRequireEmbedToken_ValidToken(t *testing.T) {
	h := newTestHandler()
	app := newTestApp(h)

	token := getToken(t, app, `{"tenant_id":"tenant_x","read_only":false}`)

	resp := doRequest(app, "GET", "/test", "", token)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]string
	json.NewDecoder(resp.Body).Decode(&result)
	assert.Equal(t, "tenant_x", result["tenant"])
}

func TestRequireEmbedToken_MissingHeader(t *testing.T) {
	h := newTestHandler()
	app := newTestApp(h)

	resp := doRequest(app, "GET", "/test", "", "")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestRequireEmbedToken_InvalidToken(t *testing.T) {
	h := newTestHandler()
	app := newTestApp(h)

	resp := doRequest(app, "GET", "/test", "", "invalid.token.here")
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestRequireEmbedToken_ExpiredToken(t *testing.T) {
	h := NewEmbedHandler(nil, nil, "test-secret-key", 24*time.Hour)
	app := fiber.New()
	app.Get("/test", h.RequireEmbedToken, func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	// Create an expired token signed with the same secret.
	claims := embedClaims{
		TenantID: "tenant_x",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-2 * time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, _ := token.SignedString([]byte("test-secret-key"))

	resp := doRequest(app, "GET", "/test", "", tokenStr)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestRequireEmbedToken_WrongSecret(t *testing.T) {
	h := NewEmbedHandler(nil, nil, "correct-secret", 24*time.Hour)
	app := fiber.New()
	app.Get("/test", h.RequireEmbedToken, func(c *fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	// Sign with a different secret.
	claims := embedClaims{
		TenantID: "tenant_x",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, _ := token.SignedString([]byte("wrong-secret"))

	resp := doRequest(app, "GET", "/test", "", tokenStr)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestRetryDelivery_ReadOnlyBlock(t *testing.T) {
	h := newTestHandler()
	app := newTestApp(h)

	token := getToken(t, app, `{"tenant_id":"tenant_x","read_only":true}`)
	resp := doRequest(app, "POST", "/test/dest123/retry", "", token)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestRetryDelivery_ReadWriteAllowed(t *testing.T) {
	h := newTestHandler()
	app := newTestApp(h)

	token := getToken(t, app, `{"tenant_id":"tenant_x","read_only":false}`)
	resp := doRequest(app, "POST", "/test/dest123/retry", "", token)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestValidateEmbedToken_AlgorithmConfusion(t *testing.T) {
	h := newTestHandler()

	// Attempt to sign with alg=none — our validation callback
	// rejects non-HMAC methods to prevent algorithm confusion attacks.
	token := jwt.NewWithClaims(jwt.SigningMethodNone, &embedClaims{
		TenantID: "tenant_x",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(24 * time.Hour)),
		},
	})
	tokenStr, _ := token.SignedString(jwt.SigningMethodNone)

	_, ok := h.validateEmbedToken(tokenStr)
	assert.False(t, ok)
}

func TestGenerateEmbedToken_TokenContainsCorrectClaims(t *testing.T) {
	h := newTestHandler()
	app := newTestApp(h)

	tokenStr := getToken(t, app, `{"tenant_id":"tenant_test","read_only":false}`)

	// Parse the token to verify claims.
	token, err := jwt.ParseWithClaims(tokenStr, &embedClaims{}, func(token *jwt.Token) (any, error) {
		return []byte("test-secret-key"), nil
	})
	require.NoError(t, err)
	require.True(t, token.Valid)

	claims := token.Claims.(*embedClaims)
	assert.Equal(t, "tenant_test", claims.TenantID)
	assert.False(t, claims.ReadOnly)
	assert.Equal(t, "pulseflow-embed", claims.Issuer)
	assert.Equal(t, "tenant_test", claims.Subject)
	assert.NotNil(t, claims.ExpiresAt)
	assert.NotNil(t, claims.IssuedAt)
}
