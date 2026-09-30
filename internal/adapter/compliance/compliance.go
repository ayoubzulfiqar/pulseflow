package compliance

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/oklog/ulid/v2"
)

// PIIRedactor implements entity.Redactor using compiled regex patterns.
// It recursively walks the event data and metadata, replacing matches
// with a configurable placeholder.
type PIIRedactor struct {
	patterns []*compiledPattern
	logger   *slog.Logger
	mu       sync.RWMutex
}

// compiledPattern holds a compiled regex and its replacement string.
type compiledPattern struct {
	name        string
	regex       *regexp.Regexp
	replacement string
}

// NewPIIRedactor creates a redactor with the given PII patterns.
func NewPIIRedactor(patterns []entity.PIIPattern, logger *slog.Logger) *PIIRedactor {
	if logger == nil {
		logger = slog.Default()
	}
	r := &PIIRedactor{logger: logger}
	r.AddPatterns(patterns)
	return r
}

// AddPatterns compiles and adds PII patterns at runtime.
func (r *PIIRedactor) AddPatterns(patterns []entity.PIIPattern) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range patterns {
		re, err := regexp.Compile(p.Pattern)
		if err != nil {
			r.logger.Warn("pii: invalid regex pattern, skipping",
				"name", p.Name, "pattern", p.Pattern, "error", err)
			continue
		}
		replacement := p.Replacement
		if replacement == "" {
			replacement = "[REDACTED]"
		}
		r.patterns = append(r.patterns, &compiledPattern{
			name:        p.Name,
			regex:       re,
			replacement: replacement,
		})
	}
}

// Redact applies all compiled patterns to the event's Data and Metadata.
// Returns a shallow copy with redacted fields.
func (r *PIIRedactor) Redact(ctx context.Context, event *entity.Event) *entity.Event {
	r.mu.RLock()
	patterns := r.patterns
	r.mu.RUnlock()

	if len(patterns) == 0 {
		return event
	}

	// Copy the event to avoid mutating the original.
	copied := &entity.Event{
		ID:        event.ID,
		Type:      event.Type,
		Source:    event.Source,
		Subject:   event.Subject,
		Version:   event.Version,
		Timestamp: event.Timestamp,
	}

	// Redact metadata.
	if event.Metadata != nil {
		copied.Metadata = make(map[string]string, len(event.Metadata))
		for k, v := range event.Metadata {
			copied.Metadata[k] = r.redactString(v)
		}
	}

	// Redact data payload.
	if len(event.Data) > 0 {
		copied.Data = r.redactJSON(event.Data)
	}

	return copied
}

// redactString applies all patterns to a string.
func (r *PIIRedactor) redactString(s string) string {
	for _, p := range r.patterns {
		s = p.regex.ReplaceAllString(s, p.replacement)
	}
	return s
}

// redactJSON recursively redacts all string values in a JSON payload.
func (r *PIIRedactor) redactJSON(data json.RawMessage) json.RawMessage {
	if len(data) == 0 {
		return data
	}

	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		// If not valid JSON, redact the raw string.
		return json.RawMessage(r.redactString(string(data)))
	}

	redacted := r.redactValue(v)
	output, err := json.Marshal(redacted)
	if err != nil {
		return data
	}
	return output
}

// redactValue recursively processes any JSON value.
func (r *PIIRedactor) redactValue(v any) any {
	switch val := v.(type) {
	case string:
		return r.redactString(val)
	case map[string]any:
		result := make(map[string]any, len(val))
		for k, v := range val {
			result[k] = r.redactValue(v)
		}
		return result
	case []any:
		result := make([]any, len(val))
		for i, v := range val {
			result[i] = r.redactValue(v)
		}
		return result
	default:
		return v
	}
}

// DefaultPIIPatterns returns a set of common PII/PHI regex patterns
// suitable for healthtech and fintech compliance.
func DefaultPIIPatterns() []entity.PIIPattern {
	return []entity.PIIPattern{
		{
			Name:        "SSN",
			Pattern:     `\b\d{3}-\d{2}-\d{4}\b`,
			Replacement: "[REDACTED_SSN]",
		},
		{
			Name:        "Email",
			Pattern:     `\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b`,
			Replacement: "[REDACTED_EMAIL]",
		},
		{
			Name:        "Credit Card",
			Pattern:     `\b(?:\d[ -]*?){13,16}\b`,
			Replacement: "[REDACTED_CC]",
		},
		{
			Name:        "Phone Number",
			Pattern:     `\+[\d\s\-\(\)]{7,}`,
			Replacement: "[REDACTED_PHONE]",
		},
		{
			Name:        "API Key",
			Pattern:     `\b(?:AKIA[0-9A-Z]{16}|sk-[a-zA-Z0-9]{20,}|pk_[a-z]+_[A-Za-z0-9]+)\b`,
			Replacement: "[REDACTED_KEY]",
		},
	}
}

// Ensure PIIRedactor satisfies entity.Redactor.
var _ entity.Redactor = (*PIIRedactor)(nil)

// AuditTrail implements entity.AuditRepository with HMAC-SHA256 signing.
type AuditTrail struct {
	auditSecret string
	logger      *slog.Logger
	records     []*entity.AuditRecord
	mu          sync.Mutex
}

// NewAuditTrail creates an audit trail with the given signing key.
func NewAuditTrail(auditSecret string, logger *slog.Logger) *AuditTrail {
	if logger == nil {
		logger = slog.Default()
	}
	return &AuditTrail{
		auditSecret: auditSecret,
		logger:      logger,
	}
}

// StoreAudit writes a cryptographically signed audit record.
func (a *AuditTrail) StoreAudit(ctx context.Context, record *entity.AuditRecord) error {
	if record.ID == "" {
		record.ID = ulid.MustNew(ulid.Timestamp(time.Now().UTC()), nil).String()
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}

	// Sign the record before storing.
	signature, err := a.signRecord(record)
	if err != nil {
		return fmt.Errorf("audit: sign record: %w", err)
	}
	record.Signature = signature

	a.mu.Lock()
	a.records = append(a.records, record)
	a.mu.Unlock()

	return nil
}

// QueryAudit returns audit records matching the filter.
func (a *AuditTrail) QueryAudit(ctx context.Context, filter entity.AuditFilter) ([]*entity.AuditRecord, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var results []*entity.AuditRecord
	for _, r := range a.records {
		if filter.DestinationID != "" && r.DestinationID != filter.DestinationID {
			continue
		}
		if filter.EventID != "" && r.EventID != filter.EventID {
			continue
		}
		if filter.Status != "" && r.Status != filter.Status {
			continue
		}
		if filter.From.After(r.Timestamp) {
			continue
		}
		if !filter.To.IsZero() && r.Timestamp.After(filter.To) {
			continue
		}
		results = append(results, r)
	}

	if filter.Limit > 0 && len(results) > filter.Limit {
		// Apply offset.
		start := 0
		if filter.Offset > 0 && filter.Offset < len(results) {
			start = filter.Offset
		}
		end := start + filter.Limit
		if end > len(results) {
			end = len(results)
		}
		results = results[start:end]
	}

	return results, nil
}

// signRecord creates an HMAC-SHA256 signature over the canonical
// JSON representation of the audit record (excluding the Signature field).
func (a *AuditTrail) signRecord(record *entity.AuditRecord) (string, error) {
	// Marshal a canonical representation (without signature).
	// We marshal the entire record, then null out the signature field.
	type signingRecord struct {
		ID            string    `json:"id"`
		EventID       string    `json:"event_id"`
		DestinationID string    `json:"destination_id"`
		Timestamp     time.Time `json:"timestamp"`
		Status        string    `json:"status"`
		StatusCode    int       `json:"status_code,omitempty"`
		Reason        string    `json:"reason,omitempty"`
		Redacted      bool      `json:"redacted"`
	}
	sr := signingRecord{
		ID:            record.ID,
		EventID:       record.EventID,
		DestinationID: record.DestinationID,
		Timestamp:     record.Timestamp,
		Status:        record.Status,
		StatusCode:    record.StatusCode,
		Reason:        record.Reason,
		Redacted:      record.Redacted,
	}

	canonical, err := json.Marshal(sr)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	mac := hmac.New(sha256.New, []byte(a.auditSecret))
	mac.Write(canonical)
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// Count returns the total number of audit records stored.
func (a *AuditTrail) Count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.records)
}

// Ensure AuditTrail satisfies entity.AuditRepository.
var _ entity.AuditRepository = (*AuditTrail)(nil)
