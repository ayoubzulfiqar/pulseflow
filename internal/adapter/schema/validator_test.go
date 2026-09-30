package schema

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newValidator() *JSONSchemaValidator {
	return NewJSONSchemaValidator(nil)
}

func testEvent(data map[string]any) *entity.Event {
	payload, _ := json.Marshal(data)
	return &entity.Event{
		ID:     entity.EventID("01HZTEST000000000000000000"),
		Type:   "test.event",
		Source: "test-source",
		Data:   payload,
	}
}

func TestSchemaValidator_ValidEvent(t *testing.T) {
	v := newValidator()
	schema := json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)

	err := v.RegisterSchema(context.Background(), &entity.SchemaDefinition{
		EventPattern: "test-source:test.event",
		Schema:       schema,
	})
	require.NoError(t, err)

	event := testEvent(map[string]any{"name": "Alice"})
	result := v.Validate(context.Background(), event)
	assert.True(t, result.Valid)
}

func TestSchemaValidator_InvalidEvent_MissingRequired(t *testing.T) {
	v := newValidator()
	schema := json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)

	err := v.RegisterSchema(context.Background(), &entity.SchemaDefinition{
		EventPattern: "test-source:test.event",
		Schema:       schema,
	})
	require.NoError(t, err)

	event := testEvent(map[string]any{"age": 30})
	result := v.Validate(context.Background(), event)
	assert.False(t, result.Valid)
}

func TestSchemaValidator_InvalidEvent_WrongType(t *testing.T) {
	v := newValidator()
	schema := json.RawMessage(`{"type":"object","properties":{"age":{"type":"number"}}}`)

	err := v.RegisterSchema(context.Background(), &entity.SchemaDefinition{
		EventPattern: "test-source:test.event",
		Schema:       schema,
	})
	require.NoError(t, err)

	event := testEvent(map[string]any{"age": "not-a-number"})
	result := v.Validate(context.Background(), event)
	assert.False(t, result.Valid)
}

func TestSchemaValidator_NoSchemaRegistered(t *testing.T) {
	v := newValidator()
	event := testEvent(map[string]any{"name": "Alice"})

	result := v.Validate(context.Background(), event)
	assert.True(t, result.Valid)
}

func TestSchemaValidator_InvalidSchema(t *testing.T) {
	v := newValidator()
	schema := json.RawMessage(`{invalid json schema}`)

	err := v.RegisterSchema(context.Background(), &entity.SchemaDefinition{
		EventPattern: "test.event",
		Schema:       schema,
	})
	assert.Error(t, err)
}

func TestSchemaValidator_ValidateWithSchema(t *testing.T) {
	v := newValidator()
	schema := json.RawMessage(`{"type":"object","required":["email"]}`)

	event := testEvent(map[string]any{"email": "test@test.com"})
	result := v.ValidateWithSchema(context.Background(), event, &entity.SchemaDefinition{
		EventPattern: "test.event",
		Schema:       schema,
	})
	assert.True(t, result.Valid)

	// Missing required field.
	event2 := testEvent(map[string]any{"name": "Bob"})
	result2 := v.ValidateWithSchema(context.Background(), event2, &entity.SchemaDefinition{
		EventPattern: "test.event",
		Schema:       schema,
	})
	assert.False(t, result2.Valid)
}

func TestSchemaValidator_GetSchema(t *testing.T) {
	v := newValidator()
	schema := json.RawMessage(`{"type":"object"}`)

	err := v.RegisterSchema(context.Background(), &entity.SchemaDefinition{
		EventPattern: "test.event",
		Schema:       schema,
		TenantID:     "test-source",
	})
	require.NoError(t, err)

	def, err := v.GetSchema(context.Background(), "test.event", "test-source")
	require.NoError(t, err)
	assert.Equal(t, "test-source", def.TenantID)
}

func TestSchemaValidator_GetSchema_NotFound(t *testing.T) {
	v := newValidator()
	_, err := v.GetSchema(context.Background(), "unknown", "unknown")
	assert.Error(t, err)
}

func TestSchemaValidator_ImplementsInterface(t *testing.T) {
	var _ entity.SchemaValidator = (*JSONSchemaValidator)(nil)
}
