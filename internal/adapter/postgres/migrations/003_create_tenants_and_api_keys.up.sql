-- 003_create_tenants_and_api_keys.up.sql
-- Multi-tenant control plane: tenants and API keys for auth.
CREATE TABLE IF NOT EXISTS tenants (
    id          TEXT      PRIMARY KEY,       -- ULID
    name        TEXT      NOT NULL UNIQUE,
    plan        TEXT      NOT NULL DEFAULT 'free',
    api_key_count INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api_keys (
    key         TEXT      PRIMARY KEY,         -- first 8 chars of the hash for lookup; full key is stored hashed
    tenant_id   TEXT      NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    label       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ,
    revoked     BOOLEAN   NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_api_keys_tenant_id ON api_keys(tenant_id);

-- Insert a default admin tenant for local development.
INSERT INTO tenants (id, name, plan)
VALUES ('01HZ0000000000000000000000', 'default', 'enterprise')
ON CONFLICT (id) DO NOTHING;
