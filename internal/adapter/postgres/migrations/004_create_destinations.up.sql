CREATE TABLE IF NOT EXISTS destinations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_pattern   TEXT        NOT NULL DEFAULT '*',
    url             TEXT        NOT NULL,
    primary_secret  TEXT        NOT NULL,
    secondary_secret TEXT,
    rotation_expires_at TIMESTAMPTZ,
    cel_filter      TEXT,
    rate_limit_rps  NUMERIC(10,3) DEFAULT 100.0,
    concurrency_limit INTEGER DEFAULT 5,
    status          TEXT        NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_destinations_event_pattern ON destinations(event_pattern);
CREATE INDEX IF NOT EXISTS idx_destinations_status ON destinations(status);
CREATE INDEX IF NOT EXISTS idx_destinations_url ON destinations(url);
