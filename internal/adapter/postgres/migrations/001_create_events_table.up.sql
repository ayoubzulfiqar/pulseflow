-- 001_create_events_table.up.sql
-- Parent events table: partitioned by month on the timestamp column for
-- high-write throughput. Partitions are created on-the-fly by the adapter.
CREATE TABLE IF NOT EXISTS events (
    id          CHAR(26)      PRIMARY KEY,
    source      TEXT          NOT NULL,
    type        TEXT          NOT NULL,
    subject     TEXT          NOT NULL,
    data        JSONB         NOT NULL,
    metadata    JSONB,
    "timestamp" TIMESTAMPTZ   NOT NULL,
    version     TEXT          NOT NULL DEFAULT 'v1',
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
) PARTITION BY RANGE ("timestamp");

-- Indexes (inherited by child partitions).
CREATE INDEX IF NOT EXISTS idx_events_source      ON events(source);
CREATE INDEX IF NOT EXISTS idx_events_type        ON events(type);
CREATE INDEX IF NOT EXISTS idx_events_subject     ON events(subject);
CREATE INDEX IF NOT EXISTS idx_events_timestamp   ON events("timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_events_source_type ON events(source, type);

-- Create the current month partition so the table is queryable immediately.
-- Future partitions are created lazily by the adapter on Store.
DO $$
DECLARE
    month_start DATE := date_trunc('month', now());
    month_end   DATE := month_start + interval '1 month';
    part_name   TEXT := 'events_' || to_char(month_start, 'YYYY_MM');
BEGIN
    EXECUTE format($sql$
        CREATE TABLE IF NOT EXISTS %I PARTITION OF events
        FOR VALUES FROM (%L) TO (%L)
    $sql$, part_name, month_start, month_end);

    EXECUTE format($sql$
        CREATE INDEX IF NOT EXISTS idx_%I_source ON %I(source)
    $sql$, part_name, part_name);

    EXECUTE format($sql$
        CREATE INDEX IF NOT EXISTS idx_%I_type ON %I(type)
    $sql$, part_name, part_name);
