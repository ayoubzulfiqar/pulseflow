package postgres

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Config configures the PostgreSQL connection pool.
type Config struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// EventRepository implements entity.EventRepository backed by PostgreSQL
// with time-based monthly partitioning for high-write throughput.
type EventRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewEventRepository creates a repository, runs migrations, and verifies
// connectivity via Ping.
func NewEventRepository(ctx context.Context, cfg Config, logger *slog.Logger) (*EventRepository, error) {
	if logger == nil {
		logger = slog.Default()
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		poolCfg.MaxConns = int32(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		poolCfg.MinConns = int32(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.ConnMaxLifetime
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	repo := &EventRepository{pool: pool, logger: logger}

	if err := repo.RunMigrations(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: migrations: %w", err)
	}

	return repo, nil
}

// Store persists an event. Ensures the monthly partition exists first.
func (r *EventRepository) Store(ctx context.Context, event *entity.Event) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("postgres: validate event: %w", err)
	}

	if err := r.ensurePartition(ctx, event.Timestamp); err != nil {
		return fmt.Errorf("postgres: ensure partition: %w", err)
	}

	metadataJSON := marshalMetadata(event.Metadata)

	_, err := r.pool.Exec(ctx,
		`INSERT INTO events (id, source, type, subject, data, metadata, "timestamp", version)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		event.ID, event.Source, event.Type, event.Subject,
		event.Data, metadataJSON, event.Timestamp, event.Version,
	)
	if err != nil {
		return fmt.Errorf("postgres: insert event: %w", err)
	}
	return nil
}

// GetByID retrieves a single event by its ULID.
func (r *EventRepository) GetByID(ctx context.Context, id entity.EventID) (*entity.Event, error) {
	event := &entity.Event{}
	var metadataJSON json.RawMessage

	err := r.pool.QueryRow(ctx,
		`SELECT id, source, type, subject, data, metadata, "timestamp", version
		 FROM events WHERE id = $1`,
		id,
	).Scan(
		&event.ID, &event.Source, &event.Type, &event.Subject,
		&event.Data, &metadataJSON, &event.Timestamp, &event.Version,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", entity.ErrNotFound, id)
		}
		return nil, fmt.Errorf("postgres: query by id: %w", err)
	}

	event.Metadata = unmarshalMetadata(metadataJSON)
	return event, nil
}

// Query retrieves events matching the filter with pagination.
func (r *EventRepository) Query(ctx context.Context, filter entity.EventFilter) ([]*entity.Event, error) {
	types := convertEventTypes(filter.Types)
	sources := filter.Sources
	subjects := filter.Subjects

	var fromVal, toVal interface{}
	if !filter.From.IsZero() {
		fromVal = filter.From
	}
	if !filter.To.IsZero() {
		toVal = filter.To
	}

	query := `
		SELECT id, source, type, subject, data, metadata, "timestamp", version
		FROM events
		WHERE ($1::text[] IS NULL OR type = ANY($1))
		  AND ($2::text[] IS NULL OR source = ANY($2))
		  AND ($3::text[] IS NULL OR subject = ANY($3))
		  AND ($4::timestamptz IS NULL OR "timestamp" >= $4)
		  AND ($5::timestamptz IS NULL OR "timestamp" <= $5)
		ORDER BY "timestamp" DESC
		LIMIT $6 OFFSET $7`

	rows, err := r.pool.Query(ctx, query, types, sources, subjects, fromVal, toVal, filter.MaxLimit, filter.Offset)
	if err != nil {
		return nil, fmt.Errorf("postgres: query: %w", err)
	}
	defer rows.Close()

	var events []*entity.Event
	for rows.Next() {
		event := &entity.Event{}
		var metadataJSON json.RawMessage
		if err := rows.Scan(
			&event.ID, &event.Source, &event.Type, &event.Subject,
			&event.Data, &metadataJSON, &event.Timestamp, &event.Version,
		); err != nil {
			return nil, fmt.Errorf("postgres: scan event: %w", err)
		}
		event.Metadata = unmarshalMetadata(metadataJSON)
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: rows error: %w", err)
	}

	return events, nil
}

// Close releases the connection pool.
func (r *EventRepository) Close() {
	r.pool.Close()
}

// Health checks database connectivity via Ping.
func (r *EventRepository) Health(ctx context.Context) bool {
	return r.pool.Ping(ctx) == nil
}

// RunMigrations executes all embedded up-migrations.
func (r *EventRepository) RunMigrations(ctx context.Context) error {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("postgres: read migrations dir: %w", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}

		content, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("postgres: read migration %s: %w", name, err)
		}

		r.logger.Debug("postgres: running migration", "file", name)
		if _, err := r.pool.Exec(ctx, string(content)); err != nil {
			return fmt.Errorf("postgres: execute migration %s: %w", name, err)
		}
	}

	return nil
}

// ensurePartition creates the monthly partition for the given timestamp
// if it does not already exist. Partition names are validated against a
// regex to prevent SQL injection.
func (r *EventRepository) ensurePartition(ctx context.Context, t time.Time) error {
	monthStart := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	monthEnd := monthStart.AddDate(0, 1, 0)
	partitionName := fmt.Sprintf("events_%s", monthStart.Format("2006_01"))

	if !partitionPattern.MatchString(partitionName) {
		return fmt.Errorf("invalid partition name: %s", partitionName)
	}

	_, err := r.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s PARTITION OF events
		FOR VALUES FROM ('%s') TO ('%s')
	`, partitionName,
		monthStart.Format("2006-01-02 15:04:05.000000"),
		monthEnd.Format("2006-01-02 15:04:05.000000"),
	))
	if err != nil {
		return fmt.Errorf("postgres: create partition %s: %w", partitionName, err)
	}
	return nil
}

// marshalMetadata serializes event metadata to JSON for storage.
func marshalMetadata(m map[string]string) []byte {
	if m == nil || len(m) == 0 {
		return []byte("null")
	}
	b, _ := json.Marshal(m)
	return b
}

// unmarshalMetadata deserializes event metadata from the database.
func unmarshalMetadata(raw json.RawMessage) map[string]string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return m
}

func convertEventTypes(types []entity.EventType) []string {
	if len(types) == 0 {
		return nil
	}
	result := make([]string, len(types))
	for i, t := range types {
		result[i] = string(t)
	}
	return result
}

var partitionPattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
