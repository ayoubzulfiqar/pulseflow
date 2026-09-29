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

// Pool returns the underlying connection pool for use by other repositories
// (e.g. TenantRepository) that share the same database.
func (r *EventRepository) Pool() *pgxpool.Pool {
	return r.pool
}

// Health checks database connectivity via Ping.
func (r *EventRepository) Health(ctx context.Context) bool {
	return r.pool.Ping(ctx) == nil
}

// --- DLQ persistence methods ---

// StoreDLQ persists a dead-lettered message to the dlq_messages table.
func (r *EventRepository) StoreDLQ(ctx context.Context, msg *entity.DLQMessage) error {
	if msg == nil {
		return fmt.Errorf("postgres: dlq message is nil")
	}

	var eventID, eventSource, eventType string
	var data json.RawMessage
	if msg.Event != nil {
		eventID = string(msg.Event.ID)
		eventSource = msg.Event.Source
		eventType = string(msg.Event.Type)
		data = msg.Event.Data
	}

	var metadataBytes []byte
	if msg.Event != nil && msg.Event.Metadata != nil {
		metadataBytes = marshalMetadata(msg.Event.Metadata)
	} else {
		metadataBytes = marshalMetadata(nil)
	}

	_, err := r.pool.Exec(ctx,
		`INSERT INTO dlq_messages (id, event_id, event_source, event_type, reason, retry_count, status, failed_at, consumer, data, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		 ON CONFLICT (id) DO UPDATE SET
			reason = EXCLUDED.reason,
			retry_count = EXCLUDED.retry_count,
			status = EXCLUDED.status,
			failed_at = EXCLUDED.failed_at,
			consumer = EXCLUDED.consumer`,
		msg.ID, eventID, eventSource, eventType,
		msg.Reason, msg.RetryCount, msg.Status, msg.FailedAt, msg.Consumer,
		data, metadataBytes,
	)
	if err != nil {
		return fmt.Errorf("postgres: insert dlq message: %w", err)
	}
	return nil
}

// QueryDLQ retrieves DLQ messages from the durable store with filtering
// and pagination support.
func (r *EventRepository) QueryDLQ(ctx context.Context, filter entity.DLQFilter) ([]*entity.DLQMessage, error) {
	types := convertEventTypes(filter.Types)
	var fromVal, toVal interface{}
	if !filter.From.IsZero() {
		fromVal = filter.From
	}
	if !filter.To.IsZero() {
		toVal = filter.To
	}

	query := `
		SELECT id, event_id, event_source, event_type, reason, retry_count,
		       status, failed_at, consumer, data, metadata
		FROM dlq_messages
		WHERE ($1::text[] IS NULL OR event_type = ANY($1))
		  AND ($2::text[] IS NULL OR event_source = ANY($2))
		  AND ($3::text[] IS NULL OR FALSE) -- subjects filter via event data
		  AND ($4::text IS NULL OR status = $4)
		  AND ($5::timestamptz IS NULL OR failed_at >= $5)
		  AND ($6::timestamptz IS NULL OR failed_at <= $6)
		  AND retry_count >= $7
		ORDER BY failed_at DESC
		LIMIT $8 OFFSET $9`

	// For subjects, we need to match against the event subject stored in metadata.
	// Simplified: filter by subject in Go after retrieval if needed.
	rows, err := r.pool.Query(ctx, query, types, filter.Sources, nil,
		string(filter.Status), fromVal, toVal, filter.MinRetry, filter.MaxLimit, filter.Offset)
	if err != nil {
		return nil, fmt.Errorf("postgres: query dlq: %w", err)
	}
	defer rows.Close()

	var msgs []*entity.DLQMessage
	for rows.Next() {
		msg, err := scanDLQRow(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan dlq row: %w", err)
		}
		msgs = append(msgs, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: dlq rows error: %w", err)
	}

	// Apply subject filtering in Go (subjects are stored in event metadata).
	if len(filter.Subjects) > 0 {
		filtered := make([]*entity.DLQMessage, 0, len(msgs))
		subjectSet := make(map[string]bool, len(filter.Subjects))
		for _, s := range filter.Subjects {
			subjectSet[s] = true
		}
		for _, m := range msgs {
			if m.Event != nil && subjectSet[m.Event.Subject] {
				filtered = append(filtered, m)
			}
		}
		msgs = filtered
	}

	return msgs, nil
}

// GetDLQByID retrieves a single DLQ message by its stream message ID.
func (r *EventRepository) GetDLQByID(ctx context.Context, id string) (*entity.DLQMessage, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT id, event_id, event_source, event_type, reason, retry_count,
		        status, failed_at, consumer, data, metadata
		 FROM dlq_messages WHERE id = $1`,
		id,
	)

	msg, err := scanDLQRow(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", entity.ErrDLQMessageNotFound, id)
		}
		return nil, fmt.Errorf("postgres: get dlq by id: %w", err)
	}
	return msg, nil
}

// UpdateDLQStatus updates the status and retry count of a DLQ record.
func (r *EventRepository) UpdateDLQStatus(ctx context.Context, id string, status entity.DLQStatus, retryCount int) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE dlq_messages SET status = $1, retry_count = $2 WHERE id = $3`,
		status, retryCount, id,
	)
	if err != nil {
		return fmt.Errorf("postgres: update dlq status: %w", err)
	}
	return nil
}

// DeleteDLQ removes DLQ records by IDs and returns the count deleted.
func (r *EventRepository) DeleteDLQ(ctx context.Context, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	// Build a parameterized IN clause.
	params := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		params[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`DELETE FROM dlq_messages WHERE id IN (%s)`, strings.Join(params, ","))
	tag, err := r.pool.Exec(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("postgres: delete dlq: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// scanDLQRow converts a database row into a DLQMessage entity.
func scanDLQRow(row interface {
	Scan(dest ...interface{}) error
}) (*entity.DLQMessage, error) {
	msg := &entity.DLQMessage{}
	var eventID, eventSource, eventType string
	var data json.RawMessage
	var metadataRaw json.RawMessage
	var consumer *string

	err := row.Scan(
		&msg.ID, &eventID, &eventSource, &eventType,
		&msg.Reason, &msg.RetryCount, &msg.Status,
		&msg.FailedAt, &consumer, &data, &metadataRaw,
	)
	if err != nil {
		return nil, err
	}

	msg.Consumer = derefString(consumer)

	if eventID != "" || eventSource != "" || eventType != "" || data != nil {
		msg.Event = &entity.Event{
			ID:       entity.EventID(eventID),
			Source:   eventSource,
			Type:     entity.EventType(eventType),
			Data:     data,
			Metadata: unmarshalMetadata(metadataRaw),
		}
	}

	if msg.FailedAt.IsZero() {
		msg.FailedAt = time.Now().UTC()
	}

	return msg, nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
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
