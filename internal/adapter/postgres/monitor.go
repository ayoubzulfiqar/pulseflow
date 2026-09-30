package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/jackc/pgx/v5/pgxpool"
)

// MonitorRepository implements entity.MonitorRepository using PostgreSQL.
// It queries the audit_records table for delivery timestamps and manages
// heartbeat expectation configuration in the heartbeat_expectations table.
type MonitorRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewMonitorRepository creates a PostgreSQL-backed monitor repository.
func NewMonitorRepository(pool *pgxpool.Pool, logger *slog.Logger) *MonitorRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &MonitorRepository{pool: pool, logger: logger}
}

// LastDeliveryTime returns the timestamp of the most recent successful
// delivery for the given destination and event type, using the
// audit_records table.
func (r *MonitorRepository) LastDeliveryTime(ctx context.Context, destID string, eventType string) (*time.Time, error) {
	var ts *time.Time
	query := `
		SELECT MAX(timestamp) FROM audit_records
		WHERE destination_id = $1 AND status = 'delivered'
	`
	args := []any{destID}

	if eventType != "" {
		query += " AND event_id IN (SELECT id FROM events WHERE type = $2)"
		args = append(args, eventType)
	}

	err := r.pool.QueryRow(ctx, query, args...).Scan(&ts)
	if err != nil {
		return nil, fmt.Errorf("postgres: last delivery time: %w", err)
	}
	return ts, nil
}

// ListHeartbeatExpectations returns all active heartbeat rules.
func (r *MonitorRepository) ListHeartbeatExpectations(ctx context.Context) ([]*entity.HeartbeatExpectation, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, destination_id, event_type, expected_interval, enabled,
		       alert_webhook_url, last_delivery_at, alerted_at
		FROM heartbeat_expectations
		WHERE enabled = true
	`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list heartbeats: %w", err)
	}
	defer rows.Close()

	var results []*entity.HeartbeatExpectation
	for rows.Next() {
		var hb entity.HeartbeatExpectation
		var id int
		var intervalStr string
		var lastDelivery, alertedAt *time.Time

		if err := rows.Scan(
			&id,
			&hb.DestinationID,
			&hb.EventType,
			&intervalStr,
			&hb.Enabled,
			&hb.AlertWebhookURL,
			&lastDelivery,
			&alertedAt,
		); err != nil {
			return nil, fmt.Errorf("postgres: scan heartbeat: %w", err)
		}

		duration, err := time.ParseDuration(intervalStr)
		if err != nil {
			// Fallback: treat as 24h.
			duration = 24 * time.Hour
		}
		hb.ExpectedInterval = duration

		if lastDelivery != nil {
			t := *lastDelivery
			hb.LastDeliveryAt = &t
		}
		if alertedAt != nil {
			t := *alertedAt
			hb.AlertedAt = &t
		}

		results = append(results, &hb)
	}

	return results, nil
}

// SaveHeartbeatExpectation inserts or updates a heartbeat rule.
func (r *MonitorRepository) SaveHeartbeatExpectation(ctx context.Context, hb *entity.HeartbeatExpectation) error {
	query := `
		INSERT INTO heartbeat_expectations
			(destination_id, event_type, expected_interval, enabled, alert_webhook_url, last_delivery_at, alerted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (destination_id) DO UPDATE SET
			event_type = EXCLUDED.event_type,
			expected_interval = EXCLUDED.expected_interval,
			enabled = EXCLUDED.enabled,
			alert_webhook_url = EXCLUDED.alert_webhook_url,
			last_delivery_at = EXCLUDED.last_delivery_at,
			alerted_at = EXCLUDED.alerted_at,
			updated_at = NOW()
	`

	var lastDelivery, alertedAt any
	if hb.LastDeliveryAt != nil {
		lastDelivery = *hb.LastDeliveryAt
	}
	if hb.AlertedAt != nil {
		alertedAt = *hb.AlertedAt
	}

	_, err := r.pool.Exec(ctx, query,
		hb.DestinationID,
		hb.EventType,
		hb.ExpectedInterval.String(),
		hb.Enabled,
		hb.AlertWebhookURL,
		lastDelivery,
		alertedAt,
	)
	if err != nil {
		return fmt.Errorf("postgres: save heartbeat: %w", err)
	}
	return nil
}

// UpdateLastDelivery updates the last delivery timestamp for a heartbeat.
func (r *MonitorRepository) UpdateLastDelivery(ctx context.Context, destID string, eventType string, ts time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE heartbeat_expectations
		SET last_delivery_at = $1, updated_at = NOW()
		WHERE destination_id = $2
	`, ts, destID)
	if err != nil {
		return fmt.Errorf("postgres: update last delivery: %w", err)
	}
	return nil
}

// MarkAlerted records that an alert was sent for a heartbeat.
func (r *MonitorRepository) MarkAlerted(ctx context.Context, destID string, ts time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE heartbeat_expectations
		SET alerted_at = $1, updated_at = NOW()
		WHERE destination_id = $2
	`, ts, destID)
	if err != nil {
		return fmt.Errorf("postgres: mark alerted: %w", err)
	}
	return nil
}

// Ensure MonitorRepository satisfies entity.MonitorRepository.
var _ entity.MonitorRepository = (*MonitorRepository)(nil)
