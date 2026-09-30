package entity

import (
	"context"
	"fmt"
	"time"
)

// HeartbeatExpectation configures the "Dead Man's Switch" alerting.
// If a destination does not receive at least one successful delivery
// for the expected event type within the time window, an alert fires.
type HeartbeatExpectation struct {
	// DestinationID is the webhook destination to monitor.
	DestinationID string

	// EventType is the event type that should arrive periodically.
	// Use "" for "any event type".
	EventType string

	// ExpectedInterval is the maximum gap allowed between successful
	// deliveries before an alert is triggered (e.g. 24h).
	ExpectedInterval time.Duration

	// Enabled controls whether this heartbeat rule is active.
	Enabled bool

	// AlertWebhookURL is the webhook URL to call when the heartbeat
	// fails (Slack, Discord, etc.).
	AlertWebhookURL string

	// LastDeliveryAt is the last known successful delivery time.
	// Updated by the monitor worker.
	LastDeliveryAt *time.Time

	// AlertedAt records when the last alert was sent, to avoid
	// duplicate alerts within the same gap window.
	AlertedAt *time.Time
}

// AlertPayload is the payload sent to AlertWebhookURL when a
// heartbeat expectation is violated.
type AlertPayload struct {
	// DestinationID is the destination that missed its heartbeat.
	DestinationID string `json:"destination_id"`

	// EventType is the expected event type.
	EventType string `json:"event_type"`

	// ExpectedInterval is the configured expectation duration.
	ExpectedInterval string `json:"expected_interval"`

	// LastDeliveryAt is when the last successful delivery occurred.
	LastDeliveryAt string `json:"last_delivery_at"`

	// Gap is the actual duration since the last delivery.
	Gap string `json:"gap"`

	// Message is a human-readable alert message.
	Message string `json:"message"`

	// TriggeredAt is when the alert was generated.
	TriggeredAt time.Time `json:"triggered_at"`
}

// AlertSender is the port for sending alert notifications.
type AlertSender interface {
	// SendAlert delivers an alert payload to the configured webhook URL.
	SendAlert(ctx context.Context, alert *AlertPayload) error
}

// MonitorRepository is the port for querying delivery history
// to evaluate heartbeat expectations.
type MonitorRepository interface {
	// LastDeliveryTime returns the timestamp of the most recent
	// successful delivery for the given destination and event type.
	LastDeliveryTime(ctx context.Context, destID string, eventType string) (*time.Time, error)

	// ListHeartbeatExpectations returns all active heartbeat rules.
	ListHeartbeatExpectations(ctx context.Context) ([]*HeartbeatExpectation, error)

	// SaveHeartbeatExpectation persists or updates a heartbeat rule.
	SaveHeartbeatExpectation(ctx context.Context, hb *HeartbeatExpectation) error

	// UpdateLastDelivery updates the last delivery time for a heartbeat.
	UpdateLastDelivery(ctx context.Context, destID string, eventType string, ts time.Time) error

	// MarkAlerted records that an alert was sent for a heartbeat.
	MarkAlerted(ctx context.Context, destID string, ts time.Time) error
}

// FormatDuration returns a human-readable duration string for alerts.
func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%d seconds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%d minutes", int(d.Minutes()))
	}
	return fmt.Sprintf("%d hours", int(d.Hours()))
}
