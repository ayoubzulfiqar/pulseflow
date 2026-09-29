package entity

import "time"

// DestinationStatus indicates whether a webhook destination is active
// or has been automatically disabled (e.g. after HTTP 410 Gone).
type DestinationStatus string

const (
	DestinationActive   DestinationStatus = "active"
	DestinationDisabled DestinationStatus = "disabled"
)

// Destination represents a webhook target endpoint with dual-secret
// rotation support, a CEL filter for conditional delivery, and
// per-destination rate/concurrency limits.
type Destination struct {
	// ID is the unique destination identifier.
	ID string `json:"id"`

	// EventPattern selects which events are delivered to this destination
	// (e.g. "user.*" or "order.created").
	EventPattern string `json:"event_pattern"`

	// URL is the target webhook endpoint.
	URL string `json:"url"`

	// PrimarySecret is the currently active HMAC secret.
	PrimarySecret string `json:"primary_secret"`

	// SecondarySecret is the previous/active secret used during
	// rotation. When non-empty, both secrets are signed simultaneously.
	SecondarySecret string `json:"secondary_secret"`

	// RotationExpiresAt is the timestamp after which the secondary
	// secret should be dropped (i.e. rotation is complete). Zero means
	// no rotation in progress.
	RotationExpiresAt time.Time `json:"rotation_expires_at"`

	// CELFilter is an optional CEL expression evaluated against the event
	// before delivery. If empty, all matching events are delivered.
	CELFilter string `json:"cel_filter"`

	// RateLimitRPS is the per-destination outbound rate limit.
	RateLimitRPS float64 `json:"rate_limit_rps"`

	// ConcurrencyLimit is the max simultaneous in-flight requests.
	ConcurrencyLimit int `json:"concurrency_limit"`

	// Status controls whether deliveries are active.
	Status DestinationStatus `json:"status"`

	// CreatedAt / UpdatedAt are audit timestamps.
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// HasSecondarySecret returns true when a rotation secret is present
// and has not yet expired.
func (d *Destination) HasSecondarySecret() bool {
	return d.SecondarySecret != "" && d.RotationExpiresAt.After(time.Now().UTC())
}

// IsDisabled returns true when the destination should not receive deliveries.
func (d *Destination) IsDisabled() bool {
	return d.Status == DestinationDisabled
}
