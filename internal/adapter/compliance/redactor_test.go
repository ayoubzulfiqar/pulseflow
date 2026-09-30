package compliance

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/stretchr/testify/assert"
)

func newRedactor() *PIIRedactor {
	return NewPIIRedactor(DefaultPIIPatterns(), nil)
}

func TestPIIRedactor_RedactsSSN(t *testing.T) {
	r := newRedactor()
	event := &entity.Event{
		Data: json.RawMessage(`{"ssn": "123-45-6789", "name": "John Doe"}`),
	}
	result := r.Redact(context.Background(), event)

	var data map[string]string
	json.Unmarshal(result.Data, &data)
	assert.Equal(t, "[REDACTED_SSN]", data["ssn"])
	assert.Equal(t, "John Doe", data["name"])
}

func TestPIIRedactor_RedactsEmail(t *testing.T) {
	r := newRedactor()
	event := &entity.Event{
		Data: json.RawMessage(`{"email": "john@example.com", "name": "John"}`),
	}
	result := r.Redact(context.Background(), event)

	var data map[string]string
	json.Unmarshal(result.Data, &data)
	assert.Equal(t, "[REDACTED_EMAIL]", data["email"])
	assert.Equal(t, "John", data["name"])
}

func TestPIIRedactor_RedactsNestedFields(t *testing.T) {
	r := newRedactor()
	event := &entity.Event{
		Data: json.RawMessage(`{"user": {"email": "test@corp.com", "phone": "+1-555-1234567"}}`),
	}
	result := r.Redact(context.Background(), event)

	var data map[string]any
	json.Unmarshal(result.Data, &data)
	user := data["user"].(map[string]any)
	assert.Equal(t, "[REDACTED_EMAIL]", user["email"])
	assert.Equal(t, "[REDACTED_PHONE]", user["phone"])
}

func TestPIIRedactor_RedactsArrayElements(t *testing.T) {
	r := newRedactor()
	event := &entity.Event{
		Data: json.RawMessage(`{"emails": ["a@test.com", "b@test.com", "normal"]}`),
	}
	result := r.Redact(context.Background(), event)

	var data map[string]any
	json.Unmarshal(result.Data, &data)
	emails := data["emails"].([]any)
	assert.Equal(t, "[REDACTED_EMAIL]", emails[0])
	assert.Equal(t, "[REDACTED_EMAIL]", emails[1])
	assert.Equal(t, "normal", emails[2])
}

func TestPIIRedactor_DoesNotModifyMetadata(t *testing.T) {
	r := newRedactor()
	event := &entity.Event{
		Data:     json.RawMessage(`{"email": "a@b.com"}`),
		Metadata: map[string]string{"trace_id": "abc123", "email": "meta@test.com"},
	}
	result := r.Redact(context.Background(), event)

	assert.Equal(t, "[REDACTED_EMAIL]", result.Metadata["email"])
	assert.Equal(t, "abc123", result.Metadata["trace_id"])
}

func TestPIIRedactor_NoPatterns_ReturnsOriginal(t *testing.T) {
	r := NewPIIRedactor(nil, nil)
	event := &entity.Event{
		Data: json.RawMessage(`{"email": "test@test.com"}`),
	}
	result := r.Redact(context.Background(), event)
	assert.Equal(t, event.Data, result.Data)
}

func TestPIIRedactor_EmptyData(t *testing.T) {
	r := newRedactor()
	event := &entity.Event{}
	result := r.Redact(context.Background(), event)
	assert.Nil(t, result.Data)
}

func TestPIIRedactor_InvalidJSON_RedactsRawString(t *testing.T) {
	r := newRedactor()
	event := &entity.Event{
		Data: json.RawMessage(`not valid json with email@test.com`),
	}
	result := r.Redact(context.Background(), event)
	assert.Contains(t, string(result.Data), "[REDACTED_EMAIL]")
}
