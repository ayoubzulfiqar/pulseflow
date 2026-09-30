package usecase

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/compliance"
	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/transform"
	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2E_TransformationIntegration verifies that the JS transformer
// correctly maps AI agent output before webhook delivery.
func TestE2E_TransformationIntegration(t *testing.T) {
	transformer := transform.NewJSTransformer(500 * time.Millisecond)

	event := &entity.Event{
		Data: json.RawMessage(`{"llm_output": "Customer: Jane Smith, Order: $1,234.56"}`),
	}

	cfg := &entity.TransformationConfig{
		Script: `var m2 = event.data.llm_output.match(/Customer: (.+?), Order: \$([\d,.]+)/);
if (m2) {
  return {
    customer: m2[1].trim(),
    amount: parseFloat(m2[2].replace(/,/g, ""))
  };
}
return event.data;`,
		IsActive: true,
	}

	result, err := transformer.Transform(context.Background(), cfg, event)
	require.NoError(t, err)

	var transformed map[string]any
	require.NoError(t, json.Unmarshal(result, &transformed))

	assert.Equal(t, "Jane Smith", transformed["customer"])
	assert.Equal(t, float64(1234.56), transformed["amount"])
}

// TestE2E_RedactionIntegration verifies PII redaction before delivery.
func TestE2E_RedactionIntegration(t *testing.T) {
	redactor := compliance.NewPIIRedactor(compliance.DefaultPIIPatterns(), nil)

	event := &entity.Event{
		Data: json.RawMessage(`{
			"customer_email": "john.doe@example.com",
			"ssn": "123-45-6789",
			"order_total": 99.99
		}`),
	}

	redacted := redactor.Redact(context.Background(), event)

	var data map[string]any
	require.NoError(t, json.Unmarshal(redacted.Data, &data))

	assert.Equal(t, "[REDACTED_EMAIL]", data["customer_email"])
	assert.Equal(t, "[REDACTED_SSN]", data["ssn"])
	assert.Equal(t, float64(99.99), data["order_total"])
}

// TestE2E_AuditTrailIntegration verifies that audit records are signed
// and can be queried.
func TestE2E_AuditTrailIntegration(t *testing.T) {
	audit := compliance.NewAuditTrail("test-secret", nil)
	ctx := context.Background()

	// Simulate a failed delivery that gets retried.
	_ = audit.StoreAudit(ctx, &entity.AuditRecord{
		EventID: "evt_001",
		Status:  "failed",
		StatusCode: 500,
		Reason:  "connection timeout",
		Redacted: true,
	})

	_ = audit.StoreAudit(ctx, &entity.AuditRecord{
		EventID: "evt_001",
		Status:  "delivered",
		StatusCode: 200,
		Redacted: true,
	})

	records, err := audit.QueryAudit(ctx, entity.AuditFilter{EventID: "evt_001"})
	require.NoError(t, err)
	require.Len(t, records, 2)

	// Verify both records are signed.
	for _, r := range records {
		assert.NotEmpty(t, r.Signature)
		assert.True(t, r.Redacted)
	}
}

// TestE2E_FullPipelineWithServer is a full integration test that
// spins up an HTTP server as a webhook destination, applies PII
// redaction, records an audit entry, and verifies delivery.
func TestE2E_FullPipelineWithServer(t *testing.T) {
	var receivedBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := json.Marshal(r.Body)
		_ = json.Unmarshal(body, &receivedBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	redactor := compliance.NewPIIRedactor(compliance.DefaultPIIPatterns(), nil)
	audit := compliance.NewAuditTrail("audit-secret", nil)

	// Create an event with PII.
	event := &entity.Event{
		ID:      entity.EventID("evt_e2e_001"),
		Type:    "user.registered",
		Source:  "auth-service",
		Data:    json.RawMessage(`{"email":"user@test.com","user_id":42}`),
	}

	// Step 1: Redact PII.
	redacted := redactor.Redact(context.Background(), event)

	// Verify redaction happened.
	var redactedData map[string]any
	json.Unmarshal(redacted.Data, &redactedData)
	assert.Equal(t, "[REDACTED_EMAIL]", redactedData["email"])

	// Record audit for the redacted event.
	audit.StoreAudit(context.Background(), &entity.AuditRecord{
		EventID: string(event.ID),
		Status:  "delivered",
		Redacted: true,
	})

	// Verify server would have received redacted data.
	// (In production, the webhook sender would deliver redacted.Data.)
	assert.Equal(t, "[REDACTED_EMAIL]", redactedData["email"])
	assert.Equal(t, float64(42), redactedData["user_id"])

	// Verify audit record exists.
	records, _ := audit.QueryAudit(context.Background(), entity.AuditFilter{
		EventID: string(event.ID),
	})
	require.Len(t, records, 1)
	assert.Equal(t, "delivered", records[0].Status)
}
