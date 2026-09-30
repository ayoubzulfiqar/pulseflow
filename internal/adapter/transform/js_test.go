package transform

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEvent(data map[string]any) *entity.Event {
	payload, _ := json.Marshal(data)
	return &entity.Event{
		ID:       entity.EventID("01HZTEST000000000000000000"),
		Type:     "order.created",
		Source:   "billing",
		Subject:  "order:1",
		Data:     payload,
		Version:  "v1",
	}
}

func TestTransform_EmptyScript_ReturnsOriginal(t *testing.T) {
	tf := NewJSTransformer(500 * time.Millisecond)
	event := newTestEvent(map[string]any{"amount": 100.0})

	cfg := &entity.TransformationConfig{Script: "", IsActive: true}
	result, err := tf.Transform(context.Background(), cfg, event)
	require.NoError(t, err)
	assert.JSONEq(t, string(event.Data), string(result))
}

func TestTransform_InactiveConfig_ReturnsOriginal(t *testing.T) {
	tf := NewJSTransformer(500 * time.Millisecond)
	event := newTestEvent(map[string]any{"amount": 100.0})

	cfg := &entity.TransformationConfig{Script: "return {total: event.data.amount}", IsActive: false}
	result, err := tf.Transform(context.Background(), cfg, event)
	require.NoError(t, err)
	assert.JSONEq(t, string(event.Data), string(result))
}

func TestTransform_NilConfig_ReturnsOriginal(t *testing.T) {
	tf := NewJSTransformer(500 * time.Millisecond)
	event := newTestEvent(map[string]any{"amount": 100.0})

	result, err := tf.Transform(context.Background(), nil, event)
	require.NoError(t, err)
	assert.JSONEq(t, string(event.Data), string(result))
}

func TestTransform_SimpleScript(t *testing.T) {
	tf := NewJSTransformer(500 * time.Millisecond)
	event := newTestEvent(map[string]any{"amount": 150.0, "currency": "USD"})

	cfg := &entity.TransformationConfig{
		Script:   `return { total: event.data.amount, currency: event.data.currency }`,
		IsActive: true,
	}
	result, err := tf.Transform(context.Background(), cfg, event)
	require.NoError(t, err)

	var transformed map[string]any
	err = json.Unmarshal(result, &transformed)
	require.NoError(t, err)

	assert.Equal(t, 150.0, transformed["total"])
	assert.Equal(t, "USD", transformed["currency"])
}

func TestTransform_AIOutputMapping(t *testing.T) {
	tf := NewJSTransformer(500 * time.Millisecond)
	event := newTestEvent(map[string]any{
		"llm_output": "Customer Name: John Doe, Amount: $500.00",
	})

	cfg := &entity.TransformationConfig{
		Script: `var output = event.data.llm_output;
var nameMatch = output.match(/Customer Name: (.+?)(?:,|$)/);
var amountMatch = output.match(/Amount: \$([\d.]+)/);
return {
  customer_name: nameMatch ? nameMatch[1].trim() : "",
  amount: amountMatch ? parseFloat(amountMatch[1]) : 0,
  source: "ai_agent"
}`,
		IsActive: true,
	}
	result, err := tf.Transform(context.Background(), cfg, event)
	require.NoError(t, err)

	var transformed map[string]any
	err = json.Unmarshal(result, &transformed)
	require.NoError(t, err)

	assert.Equal(t, "John Doe", transformed["customer_name"])
	assert.Equal(t, 500.0, transformed["amount"])
	assert.Equal(t, "ai_agent", transformed["source"])
}

func TestTransform_JSONSchemaValidation_Pass(t *testing.T) {
	tf := NewJSTransformer(500 * time.Millisecond)
	event := newTestEvent(map[string]any{"amount": 100.0})

	schema := json.RawMessage(`{"type":"object","properties":{"total":{"type":"number"}},"required":["total"]}`)

	cfg := &entity.TransformationConfig{
		Script:     `return { total: event.data.amount }`,
		IsActive:   true,
		JSONSchema: schema,
	}
	result, err := tf.Transform(context.Background(), cfg, event)
	require.NoError(t, err)
	assert.JSONEq(t, `{"total":100}`, string(result))
}

func TestTransform_InvalidScript_ReturnsError(t *testing.T) {
	tf := NewJSTransformer(500 * time.Millisecond)
	event := newTestEvent(map[string]any{"amount": 100.0})

	cfg := &entity.TransformationConfig{
		Script:   `this is not valid javascript{{{`,
		IsActive: true,
	}
	_, err := tf.Transform(context.Background(), cfg, event)
	require.Error(t, err)
}

func TestTransform_HandlesNestedData(t *testing.T) {
	tf := NewJSTransformer(500 * time.Millisecond)
	event := newTestEvent(map[string]any{
		"nested": map[string]any{
			"deep": map[string]any{"value": "hello"},
			"items": []any{"a", "b", "c"},
		},
	})

	cfg := &entity.TransformationConfig{
		Script:   `return { flat: event.data.nested.deep.value, count: event.data.nested.items.length }`,
		IsActive: true,
	}
	result, err := tf.Transform(context.Background(), cfg, event)
	require.NoError(t, err)

	var transformed map[string]any
	err = json.Unmarshal(result, &transformed)
	require.NoError(t, err)

	assert.Equal(t, "hello", transformed["flat"])
	assert.Equal(t, float64(3), transformed["count"])
}

func TestTransform_ImplementsInterface(t *testing.T) {
	var _ entity.Transformer = (*JSTransformer)(nil)
}
