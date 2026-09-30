package compliance

import (
	"context"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditTrail_StoreAudit_SetsIDAndSignature(t *testing.T) {
	at := NewAuditTrail("secret-key", nil)
	record := &entity.AuditRecord{
		EventID: "evt_123",
		Status:  "delivered",
	}
	err := at.StoreAudit(context.Background(), record)
	require.NoError(t, err)

	assert.NotEmpty(t, record.ID)
	assert.NotEmpty(t, record.Signature)
	assert.Equal(t, "evt_123", record.EventID)
}

func TestAuditTrail_StoreAudit_NilTimestampSetsNow(t *testing.T) {
	at := NewAuditTrail("secret-key", nil)
	before := time.Now().UTC()
	record := &entity.AuditRecord{EventID: "e1"}
	err := at.StoreAudit(context.Background(), record)
	require.NoError(t, err)

	assert.True(t, record.Timestamp.After(before.Add(-1*time.Second)))
}

func TestAuditTrail_QueryAudit_ByEventID(t *testing.T) {
	at := NewAuditTrail("secret-key", nil)
	ctx := context.Background()

	_ = at.StoreAudit(ctx, &entity.AuditRecord{EventID: "evt_a", Status: "delivered"})
	_ = at.StoreAudit(ctx, &entity.AuditRecord{EventID: "evt_b", Status: "failed"})
	_ = at.StoreAudit(ctx, &entity.AuditRecord{EventID: "evt_a", Status: "failed"})

	results, err := at.QueryAudit(ctx, entity.AuditFilter{EventID: "evt_a"})
	require.NoError(t, err)
	require.Len(t, results, 2)
}

func TestAuditTrail_QueryAudit_ByStatus(t *testing.T) {
	at := NewAuditTrail("secret-key", nil)
	ctx := context.Background()

	_ = at.StoreAudit(ctx, &entity.AuditRecord{EventID: "e1", Status: "delivered"})
	_ = at.StoreAudit(ctx, &entity.AuditRecord{EventID: "e2", Status: "failed"})
	_ = at.StoreAudit(ctx, &entity.AuditRecord{EventID: "e3", Status: "failed"})

	results, err := at.QueryAudit(ctx, entity.AuditFilter{Status: "failed"})
	require.NoError(t, err)
	require.Len(t, results, 2)
}

func TestAuditTrail_QueryAudit_LimitAndOffset(t *testing.T) {
	at := NewAuditTrail("secret-key", nil)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		_ = at.StoreAudit(ctx, &entity.AuditRecord{EventID: "e", Status: "delivered"})
	}

	// Limit 5, skip 2.
	results, err := at.QueryAudit(ctx, entity.AuditFilter{Limit: 5, Offset: 2})
	require.NoError(t, err)
	assert.Len(t, results, 5)
}

func TestAuditTrail_SignRecord_Deterministic(t *testing.T) {
	at := NewAuditTrail("secret-key", nil)

	rec1 := &entity.AuditRecord{EventID: "e1", Status: "delivered", Timestamp: time.Unix(1700000000, 0)}
	rec2 := &entity.AuditRecord{EventID: "e1", Status: "delivered", Timestamp: time.Unix(1700000000, 0)}

	sig1, err := at.signRecord(rec1)
	require.NoError(t, err)
	sig2, err := at.signRecord(rec2)
	require.NoError(t, err)

	assert.Equal(t, sig1, sig2)
}

func TestAuditTrail_SignRecord_DifferentSecretsDiffer(t *testing.T) {
	at1 := NewAuditTrail("secret1", nil)
	at2 := NewAuditTrail("secret2", nil)

	rec := &entity.AuditRecord{EventID: "e1", Status: "delivered", Timestamp: time.Unix(1700000000, 0)}

	sig1, err := at1.signRecord(rec)
	require.NoError(t, err)
	sig2, err := at2.signRecord(rec)
	require.NoError(t, err)

	assert.NotEqual(t, sig1, sig2)
}

func TestAuditTrail_Count(t *testing.T) {
	at := NewAuditTrail("secret-key", nil)
	ctx := context.Background()

	_ = at.StoreAudit(ctx, &entity.AuditRecord{EventID: "e1"})
	_ = at.StoreAudit(ctx, &entity.AuditRecord{EventID: "e2"})

	count := at.Count()
	assert.Equal(t, 2, count)
}

func TestAuditTrail_ImplementsInterface(t *testing.T) {
	var _ entity.AuditRepository = (*AuditTrail)(nil)
}
