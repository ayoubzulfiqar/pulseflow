package postgres

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"
)

// AuditRepository implements entity.AuditRepository using PostgreSQL
// with HMAC-SHA256 signature verification for immutable audit trails.
type AuditRepository struct {
	pool        *pgxpool.Pool
	auditSecret string
	logger      *slog.Logger
}

// NewAuditRepository creates a PostgreSQL-backed audit repository.
func NewAuditRepository(pool *pgxpool.Pool, auditSecret string, logger *slog.Logger) *AuditRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &AuditRepository{
		pool:        pool,
		auditSecret: auditSecret,
		logger:      logger,
	}
}

// StoreAudit writes a signed audit record to the audit_records table.
func (r *AuditRepository) StoreAudit(ctx context.Context, record *entity.AuditRecord) error {
	if record.ID == "" {
		record.ID = ulid.MustNew(ulid.Timestamp(time.Now().UTC()), nil).String()
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}

	// Sign the record.
	signature, err := r.signRecord(record)
	if err != nil {
		return fmt.Errorf("postgres: audit sign: %w", err)
	}
	record.Signature = signature

	_, err = r.pool.Exec(ctx, `
		INSERT INTO audit_records (id, event_id, destination_id, timestamp, status, status_code, reason, signature, redacted)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, record.ID, record.EventID, record.DestinationID, record.Timestamp,
		record.Status, record.StatusCode, record.Reason, record.Signature, record.Redacted)
	if err != nil {
		return fmt.Errorf("postgres: store audit: %w", err)
	}

	return nil
}

// QueryAudit retrieves audit records matching the filter.
func (r *AuditRepository) QueryAudit(ctx context.Context, filter entity.AuditFilter) ([]*entity.AuditRecord, error) {
	query := `
		SELECT id, event_id, destination_id, timestamp, status, status_code, reason, signature, redacted
		FROM audit_records
		WHERE 1=1
	`
	args := []any{}
	argIdx := 1

	if filter.EventID != "" {
		query += fmt.Sprintf(" AND event_id = $%d", argIdx)
		args = append(args, filter.EventID)
		argIdx++
	}
	if filter.DestinationID != "" {
		query += fmt.Sprintf(" AND destination_id = $%d", argIdx)
		args = append(args, filter.DestinationID)
		argIdx++
	}
	if filter.Status != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, filter.Status)
		argIdx++
	}
	if !filter.From.IsZero() {
		query += fmt.Sprintf(" AND timestamp >= $%d", argIdx)
		args = append(args, filter.From)
		argIdx++
	}
	if !filter.To.IsZero() {
		query += fmt.Sprintf(" AND timestamp <= $%d", argIdx)
		args = append(args, filter.To)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY timestamp DESC LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	args = append(args, limit, filter.Offset)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: query audit: %w", err)
	}
	defer rows.Close()

	var records []*entity.AuditRecord
	for rows.Next() {
		var rec entity.AuditRecord
		if err := rows.Scan(
			&rec.ID, &rec.EventID, &rec.DestinationID, &rec.Timestamp,
			&rec.Status, &rec.StatusCode, &rec.Reason, &rec.Signature, &rec.Redacted,
		); err != nil {
			return nil, fmt.Errorf("postgres: scan audit: %w", err)
		}
		records = append(records, &rec)
	}

	return records, nil
}

// signRecord creates an HMAC-SHA256 signature over the canonical JSON.
func (r *AuditRepository) signRecord(record *entity.AuditRecord) (string, error) {
	type signingRecord struct {
		ID            string    `json:"id"`
		EventID       string    `json:"event_id"`
		DestinationID string    `json:"destination_id"`
		Timestamp     time.Time `json:"timestamp"`
		Status        string    `json:"status"`
		StatusCode    int       `json:"status_code,omitempty"`
		Reason        string    `json:"reason,omitempty"`
		Redacted      bool      `json:"redacted"`
	}
	sr := signingRecord{
		ID:            record.ID,
		EventID:       record.EventID,
		DestinationID: record.DestinationID,
		Timestamp:     record.Timestamp,
		Status:        record.Status,
		StatusCode:    record.StatusCode,
		Reason:        record.Reason,
		Redacted:      record.Redacted,
	}

	canonical, err := json.Marshal(sr)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	mac := hmac.New(sha256.New, []byte(r.auditSecret))
	mac.Write(canonical)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// Count returns the total number of audit records.
func (r *AuditRepository) Count(ctx context.Context) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM audit_records").Scan(&count)
	return count, err
}

// Ensure AuditRepository satisfies entity.AuditRepository.
var _ entity.AuditRepository = (*AuditRepository)(nil)
