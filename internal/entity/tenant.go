package entity

import (
	"context"
	"time"
)

// Tenant represents an organization using the PulseFlow pipeline.
// All events and configuration are scoped to a tenant.
type Tenant struct {
	// ID is the unique tenant identifier (ULID).
	ID string

	// Name is the human-readable tenant name.
	Name string

	// Plan controls resource limits (e.g. rate limits, partition count).
	Plan string

	// APIKeyCount is the number of active API keys for this tenant.
	APIKeyCount int

	// CreatedAt is when the tenant was provisioned.
	CreatedAt time.Time
}

// TenantPlan defines the service tier.
type TenantPlan string

const (
	TenantPlanFree     TenantPlan = "free"
	TenantPlanStartup  TenantPlan = "startup"
	TenantPlanBusiness TenantPlan = "business"
	TenantPlanEnterprise TenantPlan = "enterprise"
)

// RPS returns the per-tenant rate limit based on the plan.
func (tp TenantPlan) RPS() float64 {
	switch tp {
	case TenantPlanFree:
		return 10.0
	case TenantPlanStartup:
		return 100.0
	case TenantPlanBusiness:
		return 500.0
	case TenantPlanEnterprise:
		return 5000.0
	default:
		return 10.0
	}
}

// Burst returns the burst multiplier for the plan.
func (tp TenantPlan) Burst() int {
	switch tp {
	case TenantPlanFree:
		return 10
	case TenantPlanStartup:
		return 20
	case TenantPlanBusiness:
		return 100
	case TenantPlanEnterprise:
		return 500
	default:
		return 10
	}
}

// APIKey is an authentication credential bound to a tenant.
type APIKey struct {
	// Key is the hashed or plaintext API key.
	Key string

	// TenantID is the owning tenant.
	TenantID string

	// Label is a human-readable description (e.g. "dashboard-prod").
	Label string

	// CreatedAt is when the key was issued.
	CreatedAt time.Time

	// ExpiresAt is when the key expires. Zero = never.
	ExpiresAt time.Time

	// Revoked indicates whether the key has been revoked.
	Revoked bool
}

// TenantRepository defines the persistence interface for tenant and API key
// lookups. Implemented by the PostgreSQL adapter.
type TenantRepository interface {
	// ValidateAPIKey looks up an API key and returns the associated tenant.
	// Returns ErrNotFound if the key does not exist or is revoked/expired.
	ValidateAPIKey(ctx context.Context, key string) (*Tenant, error)

	// GetTenant retrieves a tenant by ID.
	GetTenant(ctx context.Context, id string) (*Tenant, error)

	// GetTenantByName retrieves a tenant by name.
	GetTenantByName(ctx context.Context, name string) (*Tenant, error)

	// CreateTenant provisions a new tenant.
	CreateTenant(ctx context.Context, tenant *Tenant) error

	// CreateAPIKey generates and persists a new API key for the tenant.
	CreateAPIKey(ctx context.Context, key *APIKey) error

	// RevokeAPIKey marks an API key as revoked.
	RevokeAPIKey(ctx context.Context, key string) error
}
