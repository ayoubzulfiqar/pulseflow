package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PartitionManager periodically pre-creates monthly partitions for the
// events table, ensuring the upcoming N months of partitions exist before
// events arrive. This avoids write latency spikes from on-demand partition
// creation under high load.
type PartitionManager struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	interval  time.Duration
	lookahead int // number of future months to pre-create
}

// NewPartitionManager creates a PartitionManager.
func NewPartitionManager(pool *pgxpool.Pool, logger *slog.Logger, interval time.Duration, lookaheadMonths int) *PartitionManager {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = 1 * time.Hour
	}
	if lookaheadMonths <= 0 {
		lookaheadMonths = 3
	}
	return &PartitionManager{
		pool:      pool,
		logger:    logger,
		interval:  interval,
		lookahead: lookaheadMonths,
	}
}

// Run starts the background loop for pre-creating partitions.
// Blocks until ctx is cancelled.
func (m *PartitionManager) Run(ctx context.Context) error {
	m.logger.Info("partition manager: starting",
		"interval", m.interval, "lookahead_months", m.lookahead)

	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	// Create partitions immediately on startup.
	if err := m.ensureFuturePartitions(ctx); err != nil {
		m.logger.Error("partition manager: initial partition creation failed", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("partition manager: shutting down")
			return ctx.Err()
		case <-ticker.C:
			if err := m.ensureFuturePartitions(ctx); err != nil {
				m.logger.Error("partition manager: partition creation failed", "error", err)
			}
		}
	}
}

// ensureFuturePartitions creates partitions for the current and future months.
func (m *PartitionManager) ensureFuturePartitions(ctx context.Context) error {
	now := time.Now().UTC()

	for i := 0; i <= m.lookahead; i++ {
		monthStart := time.Date(now.Year(), now.Month()+time.Month(i), 1, 0, 0, 0, 0, time.UTC)
		monthEnd := monthStart.AddDate(0, 1, 0)
		partitionName := fmt.Sprintf("events_%s", monthStart.Format("2006_01"))

		if !partitionPattern.MatchString(partitionName) {
			return fmt.Errorf("invalid partition name: %s", partitionName)
		}

		_, err := m.pool.Exec(ctx, fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s PARTITION OF events
			FOR VALUES FROM ('%s') TO ('%s')
		`, partitionName,
			monthStart.Format("2006-01-02 15:04:05.000000"),
			monthEnd.Format("2006-01-02 15:04:05.000000"),
		))
		if err != nil {
			return fmt.Errorf("postgres: create partition %s: %w", partitionName, err)
		}

		// Create indexes on the partition if they don't exist.
		_, _ = m.pool.Exec(ctx, fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS idx_%s_source ON %s(source)
		`, partitionName, partitionName))
		_, _ = m.pool.Exec(ctx, fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS idx_%s_type ON %s(type)
		`, partitionName, partitionName))
		_, _ = m.pool.Exec(ctx, fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS idx_%s_timestamp ON %s("timestamp" DESC)
		`, partitionName, partitionName))

		m.logger.Debug("partition manager: ensured partition", "partition", partitionName)
	}

	return nil
}
