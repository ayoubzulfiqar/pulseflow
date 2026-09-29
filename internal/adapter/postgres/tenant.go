package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TenantRepository implements entity.TenantRepository backed by PostgreSQL.
type TenantRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewTenantRepository creates a TenantRepository.
func NewTenantRepository(pool *pgxpool.Pool, logger *slog.Logger) *TenantRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &TenantRepository{pool: pool, logger: logger}
}

// ValidateAPIKey looks up an API key by its SHA-256 hash and returns the
// associated tenant. Keys are hashed for storage; the hash of the incoming
// key is compared rather than the raw key.
func (r *TenantRepository) ValidateAPIKey(ctx context.Context, key string) (*entity.Tenant, error) {
	hash := hashAPIKey(key)

	row := r.pool.QueryRow(ctx,
		`SELECT t.id, t.name, t.plan, t.api_key_count, t.created_at
		 FROM api_keys ak
		 JOIN tenants t ON ak.tenant_id = t.id
		 WHERE ak.key = $1 AND ak.revoked = FALSE
		   AND (ak.expires_at IS NULL OR ak.expires_at > NOW())`,
		hash,
	)

	var t entity.Tenant
	var plan string
	var apiKeyCount int

	err := row.Scan(&t.ID, &t.Name, &plan, &apiKeyCount, &t.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: api key not found or expired", entity.ErrNotFound)
		}
		return nil, fmt.Errorf("postgres: validate api key: %w", err)
	}

	t.Plan = plan
	t.APIKeyCount = apiKeyCount
	return &t, nil
}

// GetTenant retrieves a tenant by ID.
func (r *TenantRepository) GetTenant(ctx context.Context, id string) (*entity.Tenant, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT id, name, plan, api_key_count, created_at FROM tenants WHERE id = $1`,
		id,
	)

	var t entity.Tenant
	var plan string
	if err := row.Scan(&t.ID, &t.Name, &plan, &t.APIKeyCount, &t.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", entity.ErrNotFound, id)
		}
		return nil, fmt.Errorf("postgres: get tenant: %w", err)
	}
	t.Plan = plan
	return &t, nil
}

// GetTenantByName retrieves a tenant by name.
func (r *TenantRepository) GetTenantByName(ctx context.Context, name string) (*entity.Tenant, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT id, name, plan, api_key_count, created_at FROM tenants WHERE name = $1`,
		name,
	)

	var t entity.Tenant
	var plan string
	if err := row.Scan(&t.ID, &t.Name, &plan, &t.APIKeyCount, &t.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: tenant name %s", entity.ErrNotFound, name)
		}
		return nil, fmt.Errorf("postgres: get tenant by name: %w", err)
	}
	t.Plan = plan
	return &t, nil
}

// CreateTenant provisions a new tenant.
func (r *TenantRepository) CreateTenant(ctx context.Context, tenant *entity.Tenant) error {
	if tenant.ID == "" {
		tenant.ID = generateTenantID()
	}
	if tenant.Plan == "" {
		tenant.Plan = string(entity.TenantPlanFree)
	}
	tenant.CreatedAt = time.Now().UTC()

	_, err := r.pool.Exec(ctx,
		`INSERT INTO tenants (id, name, plan, created_at) VALUES ($1, $2, $3, $4)`,
		tenant.ID, tenant.Name, tenant.Plan, tenant.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("postgres: create tenant: %w", err)
	}
	return nil
}

// CreateAPIKey generates and persists a new API key for the tenant.
// The key field on the APIKey struct is populated with the plaintext key
// so the caller can return it to the user. The stored value is the SHA-256
// hash.
func (r *TenantRepository) CreateAPIKey(ctx context.Context, key *entity.APIKey) error {
	if key.Key == "" {
		key.Key = generateAPIKey()
	}
	key.CreatedAt = time.Now().UTC()
	hash := hashAPIKey(key.Key)

	_, err := r.pool.Exec(ctx,
		`INSERT INTO api_keys (key, tenant_id, label, created_at, expires_at, revoked)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		hash, key.TenantID, key.Label, key.CreatedAt, key.ExpiresAt, key.Revoked,
	)
	if err != nil {
		return fmt.Errorf("postgres: create api key: %w", err)
	}

	// Update the tenant's API key count.
	_, _ = r.pool.Exec(ctx,
		`UPDATE tenants SET api_key_count = api_key_count + 1 WHERE id = $1`,
		key.TenantID,
	)

	return nil
}

// RevokeAPIKey marks an API key as revoked.
func (r *TenantRepository) RevokeAPIKey(ctx context.Context, key string) error {
	hash := hashAPIKey(key)
	_, err := r.pool.Exec(ctx,
		`UPDATE api_keys SET revoked = TRUE WHERE key = $1`,
		hash,
	)
	if err != nil {
		return fmt.Errorf("postgres: revoke api key: %w", err)
	}
	return nil
}

// hashAPIKey computes the SHA-256 hex hash of an API key for storage and lookup.
func hashAPIKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// generateTenantID creates a unique tenant ID.
func generateTenantID() string {
	h := sha256.Sum256([]byte(fmt.Sprintf("tenant_%d", time.Now().UnixNano())))
	return "tenant_" + hex.EncodeToString(h[:])[:24]
}

// generateAPIKey generates a random-looking API key string.
func generateAPIKey() string {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(time.Now().UnixNano() % 256)
	}
	return "pk_live_" + hex.EncodeToString(raw)
}

// Ensure TenantRepository satisfies entity.TenantRepository.
var _ entity.TenantRepository = (*TenantRepository)(nil)
