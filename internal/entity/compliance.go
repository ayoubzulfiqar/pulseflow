package entity

import (
	"context"
	"time"
)

// PIIPattern defines a single PII/PHI redaction rule.
type PIIPattern struct {
	// Name is a human-readable label for the pattern (e.g. "SSN").
	Name string `json:"name"`

	// Pattern is a Go-compatible regex string applied to all string
	// values in the event payload and metadata.
	Pattern string `json:"pattern"`

	// Replacement is the placeholder substituted for matched values.
	// Defaults to "[REDACTED]" if empty.
	Replacement string `json:"replacement,omitempty"`
}

// ComplianceConfig defines the compliance settings for an event pipeline.
type ComplianceConfig struct {
	// PIIPatterns is the set of regex patterns used to redact
	// PII/PHI from event data and metadata before persistence
	// and delivery.
	PIIPatterns []PIIPattern `json:"pii_patterns"`

	// RedactDLQ controls whether DLQ messages are redacted before
	// being written to PostgreSQL.
	RedactDLQ bool `json:"redact_dlq"`

	// RedactLogs controls whether log entries redact PII/PHI.
	RedactLogs bool `json:"redact_logs"`

	// AuditSigned controls whether every delivery attempt is
	// cryptographically signed for immutable audit proof.
	AuditSigned bool `json:"audit_signed"`
}

// Redactor is the port (interface) for PII/PHI redaction.
// Implementations apply regex-based redaction to event data.
type Redactor interface {
	// Redact applies redaction rules to the event's Data and Metadata
	// fields. The original event is not modified; a copy is returned.
	Redact(ctx context.Context, event *Event) *Event
}

// AuditRecord represents an immutable, cryptographically signed
// record of a single webhook delivery attempt.
type AuditRecord struct {
	// ID is a ULID for the audit entry.
	ID string `json:"id"`

	// EventID is the ULID of the event that was delivered.
	EventID string `json:"event_id"`

	// DestinationID is the webhook destination that received the delivery.
	DestinationID string `json:"destination_id"`

	// Timestamp is when the delivery attempt occurred.
	Timestamp time.Time `json:"timestamp"`

	// Status is the delivery outcome: "delivered", "failed", "skipped".
	Status string `json:"status"`

	// StatusCode is the HTTP status code from the destination (if applicable).
	StatusCode int `json:"status_code,omitempty"`

	// Reason provides additional context on failures or skips.
	Reason string `json:"reason,omitempty"`

	// Signature is the HMAC-SHA256 of the canonical JSON record,
	// signed with the pipeline's audit secret.
	Signature string `json:"signature"`

	// Redacted indicates whether PII/PHI was redacted from this record.
	Redacted bool `json:"redacted"`
}

// AuditRepository is the port for persisting immutable audit records.
type AuditRepository interface {
	// StoreAudit writes a signed audit record to the durable store.
	StoreAudit(ctx context.Context, record *AuditRecord) error

	// QueryAudit retrieves audit records matching the filter.
	QueryAudit(ctx context.Context, filter AuditFilter) ([]*AuditRecord, error)
}

// AuditFilter defines query parameters for audit records.
type AuditFilter struct {
	EventID       string
	DestinationID string
	Status        string
	From          time.Time
	To            time.Time
	Limit         int
	Offset        int
}
