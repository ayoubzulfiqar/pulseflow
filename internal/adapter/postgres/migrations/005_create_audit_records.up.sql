CREATE TABLE audit_records (
    id UUID PRIMARY KEY,
    event_id TEXT NOT NULL,
    destination_id TEXT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL CHECK (status IN ('delivering', 'delivered', 'failed', 'filtered', 'disabled')),
    status_code INTEGER,
    reason TEXT,
    signature TEXT NOT NULL,
    redacted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_event_id ON audit_records(event_id);
CREATE INDEX idx_audit_destination_id ON audit_records(destination_id);
CREATE INDEX idx_audit_status ON audit_records(status);
CREATE INDEX idx_audit_timestamp ON audit_records(timestamp DESC);
