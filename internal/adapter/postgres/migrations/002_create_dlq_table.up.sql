-- 002_create_dlq_table.up.sql
-- Dead-letter queue audit table for durable persistence of failed messages.
-- This table stores DLQ entries that are also in the Redis DLQ stream,
-- providing a durable record for operator management and audit.
CREATE TABLE IF NOT EXISTS dlq_messages (
    id          TEXT         NOT NULL PRIMARY KEY,  -- Redis stream message ID
    event_id    CHAR(26)     NOT NULL,
    event_source TEXT,
    event_type  TEXT,
    reason      TEXT         NOT NULL,
    retry_count INTEGER      NOT NULL DEFAULT 0,
    status      TEXT         NOT NULL DEFAULT 'pending',
    failed_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    consumer    TEXT,
    data        JSONB,
    metadata    JSONB
);

CREATE INDEX IF NOT EXISTS idx_dlq_status    ON dlq_messages(status);
CREATE INDEX IF NOT EXISTS idx_dlq_reason    ON dlq_messages(reason);
CREATE INDEX IF NOT EXISTS idx_dlq_event_id  ON dlq_messages(event_id);
