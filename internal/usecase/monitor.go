package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
)

// AlertSenderImpl implements entity.AlertSender using HTTP POST.
type AlertSenderImpl struct {
	client *http.Client
	logger *slog.Logger
}

// NewAlertSender creates an HTTP-based alert sender.
func NewAlertSender(logger *slog.Logger) *AlertSenderImpl {
	if logger == nil {
		logger = slog.Default()
	}
	return &AlertSenderImpl{
		client: &http.Client{Timeout: 10 * time.Second},
		logger: logger,
	}
}

// SendAlert delivers an alert payload to the configured webhook URL.
func (s *AlertSenderImpl) SendAlert(ctx context.Context, alert *entity.AlertPayload) error {
	payload, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("monitor: marshal alert: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, alert.DestinationID, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("monitor: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		s.logger.Error("monitor: send alert failed",
			"dest_id", alert.DestinationID, "error", err)
		return fmt.Errorf("monitor: send alert: %w", err)
	}
	_ = resp
	return nil
}

// Monitor is the "Dead Man's Switch" usecase. It runs a background worker
// that periodically checks for destinations that haven't received events
// within their configured heartbeat expectation, and fires alerts.
type Monitor struct {
	logger      *slog.Logger
	repo        entity.MonitorRepository
	alertSender entity.AlertSender
	interval    time.Duration
	stopCh      chan struct{}
}

// NewMonitor creates a new monitor with the given repository, alert sender,
// and check interval (how often to evaluate heartbeat expectations).
func NewMonitor(repo entity.MonitorRepository, alertSender entity.AlertSender, interval time.Duration, logger *slog.Logger) *Monitor {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	return &Monitor{
		logger:      logger,
		repo:        repo,
		alertSender: alertSender,
		interval:    interval,
		stopCh:      make(chan struct{}),
	}
}

// Start begins the background monitoring loop. Call Stop() to shut down.
func (m *Monitor) Start() {
	m.logger.Info("monitor: starting dead man's switch watcher",
		"check_interval", m.interval)
	go m.run()
}

// Stop signals the monitoring loop to shut down.
func (m *Monitor) Stop() {
	close(m.stopCh)
}

// run is the main monitoring loop. It periodically queries all active
// heartbeat expectations and fires alerts for those that have exceeded
// their expected interval without a delivery.
func (m *Monitor) run() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.checkExpectations()
		}
	}
}

// checkExpectations evaluates all active heartbeat rules and fires alerts
// for destinations that have missed their expected delivery window.
func (m *Monitor) checkExpectations() {
	ctx := context.Background()

	hbs, err := m.repo.ListHeartbeatExpectations(ctx)
	if err != nil {
		m.logger.Error("monitor: failed to list heartbeat expectations", "error", err)
		return
	}

	now := time.Now().UTC()

	for _, hb := range hbs {
		if !hb.Enabled {
			continue
		}

		lastDelivery, err := m.repo.LastDeliveryTime(ctx, hb.DestinationID, hb.EventType)
		if err != nil {
			m.logger.Warn("monitor: failed to get last delivery time",
				"dest_id", hb.DestinationID, "error", err)
			continue
		}

		if lastDelivery == nil {
			// Never received a delivery — skip unless we have an initial grace period.
			// We only alert if we've been monitoring for longer than the expected interval.
			continue
		}

		gap := now.Sub(*lastDelivery)
		if gap < hb.ExpectedInterval {
			// Still within the expected window.
			continue
		}

		// Check if we already alerted for this gap to avoid spamming.
		if hb.AlertedAt != nil && now.Sub(*hb.AlertedAt) < hb.ExpectedInterval {
			continue
		}

		// Fire the alert.
		alert := &entity.AlertPayload{
			DestinationID:    hb.DestinationID,
			EventType:        hb.EventType,
			ExpectedInterval: entity.FormatDuration(hb.ExpectedInterval),
			LastDeliveryAt:   lastDelivery.Format(time.RFC3339),
			Gap:              entity.FormatDuration(gap),
			Message: fmt.Sprintf(
				"Dead man's switch: destination %s has not received %s events in %s (expected at least every %s)",
				hb.DestinationID, hb.EventType, entity.FormatDuration(gap), entity.FormatDuration(hb.ExpectedInterval),
			),
			TriggeredAt: now,
		}

		if err := m.alertSender.SendAlert(ctx, alert); err != nil {
			m.logger.Error("monitor: failed to send alert",
				"dest_id", hb.DestinationID, "error", err)
			continue
		}

		hb.AlertedAt = &now
		_ = m.repo.MarkAlerted(ctx, hb.DestinationID, now)
		m.logger.Info("monitor: alert sent",
			"dest_id", hb.DestinationID, "gap", entity.FormatDuration(gap))
	}
}
