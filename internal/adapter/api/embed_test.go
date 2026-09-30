package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitToken(t *testing.T) {
	parts := splitToken("abc:123:sig", ':')
	assert.Equal(t, []string{"abc", "123", "sig"}, parts)
}

func TestSplitToken_EmptyString(t *testing.T) {
	parts := splitToken("", ':')
	assert.Equal(t, []string{""}, parts)
}

func TestSplitToken_SingleValue(t *testing.T) {
	parts := splitToken("noseparator", ':')
	assert.Equal(t, []string{"noseparator"}, parts)
}

func TestParseInt64_Valid(t *testing.T) {
	n, err := parseInt64("1700000000")
	assert.NoError(t, err)
	assert.Equal(t, int64(1700000000), n)
}

func TestParseInt64_Invalid(t *testing.T) {
	_, err := parseInt64("notanumber")
	assert.Error(t, err)
}

func TestParseInt64_Empty(t *testing.T) {
	n, err := parseInt64("")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

func TestEmbedHandler_GenerateAndValidateToken(t *testing.T) {
	h := NewEmbedHandler(nil, nil, "my-secret", 0)

	token := h.generateEmbedToken("tenant_1", 9999999999)
	assert.Contains(t, token, "tenant_1")

	tenantID, ok := h.validateEmbedToken(token)
	assert.True(t, ok)
	assert.Equal(t, "tenant_1", tenantID)
}

func TestEmbedHandler_ValidateToken_InvalidSignature(t *testing.T) {
	h := NewEmbedHandler(nil, nil, "my-secret", 0)

	_, ok := h.validateEmbedToken("tenant_1:9999999999:tampered")
	assert.False(t, ok)
}

func TestEmbedHandler_ValidateToken_WrongSecret(t *testing.T) {
	h1 := NewEmbedHandler(nil, nil, "secret-a", 0)
	h2 := NewEmbedHandler(nil, nil, "secret-b", 0)

	token := h1.generateEmbedToken("tenant_1", 9999999999)
	tenantID, ok := h2.validateEmbedToken(token)
	assert.False(t, ok)
	assert.Empty(t, tenantID)
}

func TestEmbedHandler_ValidateToken_Expired(t *testing.T) {
	h := NewEmbedHandler(nil, nil, "my-secret", 0)

	// Use a past expiry.
	token := h.generateEmbedToken("tenant_1", 1)
	_, ok := h.validateEmbedToken(token)
	assert.False(t, ok)
}

func TestEmbedHandler_ValidateToken_WrongFormat(t *testing.T) {
	h := NewEmbedHandler(nil, nil, "my-secret", 0)
	_, ok := h.validateEmbedToken("nope")
	assert.False(t, ok)
}

func TestEmbedHandler_ValidateToken_Empty(t *testing.T) {
	h := NewEmbedHandler(nil, nil, "my-secret", 0)
	_, ok := h.validateEmbedToken("")
	assert.False(t, ok)
}
