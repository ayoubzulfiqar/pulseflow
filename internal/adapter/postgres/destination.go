package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DestinationRepository implements entity.DestinationRepository using PostgreSQL.
// It uses the pgx connection pool for consistency with the rest of the
// postgres adapter layer.
type DestinationRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewDestinationRepository creates a PostgreSQL-backed destination repository.
func NewDestinationRepository(pool *pgxpool.Pool, logger *slog.Logger) *DestinationRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &DestinationRepository{
		pool:   pool,
		logger: logger,
	}
}

// ListActive returns all active destinations whose EventPattern matches
// the given event type and source. The EventPattern supports SQL LIKE
// syntax (e.g. "order.%") or "*" for all events.
func (r *DestinationRepository) ListActive(ctx context.Context, eventType entity.EventType, source string) ([]*entity.Destination, error) {
	pattern := patternFromEventType(string(eventType))

	rows, err := r.pool.Query(ctx, `
		SELECT id, event_pattern, url, primary_secret, secondary_secret,
		       rotation_expires_at, cel_filter, rate_limit_rps,
		       concurrency_limit, status, created_at, updated_at
		FROM destinations
		WHERE status = 'active'
		  AND ($1::text LIKE event_pattern OR event_pattern = '*')
		ORDER BY created_at ASC
	`, pattern)
	if err != nil {
		return nil, fmt.Errorf("postgres: list destinations: %w", err)
	}
	defer rows.Close()

	var dests []*entity.Destination
	for rows.Next() {
		var d entity.Destination
		var rotationExpires sql.NullTime
		var celFilter sql.NullString
		var secondarySecret sql.NullString
		var rateLimitRPS sql.NullFloat64
		var concurrencyLimit sql.NullInt32
		var status string

		if err := rows.Scan(
			&d.ID, &d.EventPattern, &d.URL, &d.PrimarySecret, &secondarySecret,
			&rotationExpires, &celFilter, &rateLimitRPS,
			&concurrencyLimit, &status, &d.CreatedAt, &d.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("postgres: scan destination: %w", err)
		}

		d.Status = entity.DestinationStatus(status)
		if secondarySecret.Valid {
			d.SecondarySecret = secondarySecret.String
		}
		if rotationExpires.Valid {
			d.RotationExpiresAt = rotationExpires.Time
		}
		if celFilter.Valid {
			d.CELFilter = celFilter.String
		}
		if rateLimitRPS.Valid {
			d.RateLimitRPS = rateLimitRPS.Float64
		}
		if concurrencyLimit.Valid {
			d.ConcurrencyLimit = int(concurrencyLimit.Int32)
		}

		dests = append(dests, &d)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: rows error: %w", err)
	}

	return dests, nil
}

// GetDestination returns a single destination by ID.
func (r *DestinationRepository) GetDestination(ctx context.Context, id string) (*entity.Destination, error) {
	var d entity.Destination
	var rotationExpires sql.NullTime
	var celFilter sql.NullString
	var secondarySecret sql.NullString
	var rateLimitRPS sql.NullFloat64
	var concurrencyLimit sql.NullInt32
	var status string

	err := r.pool.QueryRow(ctx, `
		SELECT id, event_pattern, url, primary_secret, secondary_secret,
		       rotation_expires_at, cel_filter, rate_limit_rps,
		       concurrency_limit, status, created_at, updated_at
		FROM destinations
		WHERE id = $1
	`, id).Scan(
		&d.ID, &d.EventPattern, &d.URL, &d.PrimarySecret, &secondarySecret,
		&rotationExpires, &celFilter, &rateLimitRPS,
		&concurrencyLimit, &status, &d.CreatedAt, &d.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, entity.ErrNotFound
		}
		return nil, fmt.Errorf("postgres: get destination: %w", err)
	}

	d.Status = entity.DestinationStatus(status)
	if secondarySecret.Valid {
		d.SecondarySecret = secondarySecret.String
	}
	if rotationExpires.Valid {
		d.RotationExpiresAt = rotationExpires.Time
	}
	if celFilter.Valid {
		d.CELFilter = celFilter.String
	}
	if rateLimitRPS.Valid {
		d.RateLimitRPS = rateLimitRPS.Float64
	}
	if concurrencyLimit.Valid {
		d.ConcurrencyLimit = int(concurrencyLimit.Int32)
	}

	return &d, nil
}

// DisableDestination sets status = 'disabled' for the given destination.
// Called automatically when a destination returns HTTP 410 Gone.
// Idempotent — returns nil if already disabled or not found.
func (r *DestinationRepository) DisableDestination(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE destinations
		SET status = 'disabled', updated_at = NOW()
		WHERE id = $1 AND status = 'active'
	`, id)
	if err != nil {
		return fmt.Errorf("postgres: disable destination: %w", err)
	}
	return nil
}

// patternFromEventType returns the event type as a SQL LIKE pattern.
// Matching is done via ($1::text LIKE event_pattern OR event_pattern = '*')
// in the query.
func patternFromEventType(eventType string) string {
	return eventType
}

// Ensure DestinationRepository satisfies entity.DestinationRepository.
var _ entity.DestinationRepository = (*DestinationRepository)(nil)
