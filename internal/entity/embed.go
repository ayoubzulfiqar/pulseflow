package entity

// EmbedMode controls how the embeddable white-label component renders.
type EmbedMode string

const (
	// EmbedModeFull renders the complete dashboard (iframe target).
	EmbedModeFull EmbedMode = "full"
	// EmbedModeWidget renders a single compact widget.
	EmbedModeWidget EmbedMode = "widget"
)

// EmbedTheme defines the visual theme for the white-label component.
type EmbedTheme struct {
	// PrimaryColor is the brand color for buttons and accents.
	PrimaryColor string `json:"primary_color" yaml:"primary_color"`

	// AccentColor is used for highlights and icons.
	AccentColor string `json:"accent_color,omitempty" yaml:"accent_color,omitempty"`

	// LogoURL is an optional logo image (URL or base64 data URI).
	LogoURL string `json:"logo_url,omitempty" yaml:"logo_url,omitempty"`

	// BorderRadius controls corner rounding in pixels.
	BorderRadius string `json:"border_radius,omitempty" yaml:"border_radius,omitempty"`

	// Font is the font family name (e.g. "Inter", "Roboto").
	Font string `json:"font,omitempty" yaml:"font,omitempty"`
}

// EmbedConfig defines how the white-label component is configured
// for a SaaS partner's tenant.
type EmbedConfig struct {
	// TenantID scopes all data to the partner's tenant.
	TenantID string `json:"tenant_id"`

	// Mode controls rendering ("full" or "widget").
	Mode EmbedMode `json:"mode"`

	// Theme holds brand styling for white-label appearance.
	Theme EmbedTheme `json:"theme"`

	// HideBranding removes "Powered by PulseFlow" from the UI.
	HideBranding bool `json:"hide_branding"`

	// AllowedOrigins lists domains allowed to iframe the component.
	// Empty means allow from any origin (not recommended for production).
	AllowedOrigins []string `json:"allowed_origins,omitempty"`

	// Features toggles for the embeddable component.
	ShowDeliveryLogs    bool `json:"show_delivery_logs"`
	ShowRetryButton     bool `json:"show_retry_button"`
	ShowEndpointConfig  bool `json:"show_endpoint_config"`
	ShowEventHistory    bool `json:"show_event_history"`

	// ReadOnly disables all write operations (retry, config changes).
	ReadOnly bool `json:"read_only"`

	// DefaultTimeRange is the initial time window to display (e.g. "24h").
	DefaultTimeRange string `json:"default_time_range,omitempty"`
}

// EmbedRepository defines the persistence interface for embed configs.
type EmbedRepository interface {
	// GetConfig returns the embed configuration for a tenant.
	GetConfig(tenantID string) (*EmbedConfig, error)

	// SaveConfig persists an embed configuration.
	SaveConfig(config *EmbedConfig) error

	// ValidateOrigin checks if a given origin is allowed to embed.
	ValidateOrigin(tenantID, origin string) bool
}

// EmbedToken is a short-lived token for the embeddable component,
// scoped to a tenant and optionally a specific endpoint.
type EmbedToken struct {
	// Token is the bearer token for API calls.
	Token string `json:"token"`

	// TenantID is the tenant this token is scoped to.
	TenantID string `json:"tenant_id"`

	// ExpiresAt is the token expiry timestamp.
	ExpiresAt int64 `json:"expires_at"`

	// ReadOnly indicates the token only allows read operations.
	ReadOnly bool `json:"read_only"`
}
