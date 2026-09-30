-- 007_create_heartbeat_expectations.up.sql
-- Heartbeat expectations for the Dead Man's Switch alerting wedge.
-- Each row represents a destination that should receive at least one
-- successful delivery within the expected_interval window.
CREATE TABLE IF NOT EXISTS heartbeat_expectations (
    id              SERIAL PRIMARY KEY,
    destination_id  TEXT NOT NULL,
    event_type      TEXT,
    expected_interval INTERVAL NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    alert_webhook_url TEXT,
    last_delivery_at TIMESTAMPTZ,
    alerted_at       TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_heartbeat_dest ON heartbeat_expectations(destination_id);
CREATE INDEX IF NOT EXISTS idx_heartbeat_enabled ON heartbeat_expectations(enabled);
