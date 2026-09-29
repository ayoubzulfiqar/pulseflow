package usecase

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
)

// WebhookConfig holds the configuration for a single outgoing webhook endpoint.
type WebhookConfig struct {
	URL        string
	Secret     string
	Timeout    time.Duration
	Retries    int
	RetryDelay time.Duration
}

// WebhookSender delivers signed event notifications to configured endpoints.
// Each payload is signed with HMAC-SHA256 and includes a timestamp header
// to mitigate replay attacks.
type WebhookSender struct {
	endpoints []WebhookConfig
	logger    *slog.Logger
}

// NewWebhookSender creates a WebhookSender from the application config.
func NewWebhookSender(endpoints []WebhookConfig, logger *slog.Logger) *WebhookSender {
	if logger == nil {
		logger = slog.Default()
	}
	return &WebhookSender{
		endpoints: endpoints,
		logger:    logger,
	}
}

// Notify asynchronously delivers the event to all configured endpoints.
// Failures are logged but do not block the caller. This decouples event
// ingestion from downstream notification latency.
func (w *WebhookSender) Notify(ctx context.Context, event *entity.Event) {
	for _, ep := range w.endpoints {
		go w.send(ctx, ep, event)
	}
}

func (w *WebhookSender) send(ctx context.Context, ep WebhookConfig, event *entity.Event) {
	payload, err := json.Marshal(event)
	if err != nil {
		w.logger.Error("webhook: marshal event", "error", err, "endpoint", ep.URL)
		return
	}

	ts := time.Now().Unix()
	sig := signPayload(ep.Secret, ts, payload)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.URL, strings.NewReader(string(payload)))
	if err != nil {
		w.logger.Error("webhook: create request", "error", err, "endpoint", ep.URL)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-PulseFlow-Timestamp", fmt.Sprintf("%d", ts))
	req.Header.Set("X-PulseFlow-Signature", sig)

	client := &http.Client{Timeout: ep.Timeout}

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = ep.RetryDelay
	bo.Multiplier = 2.0

	attempts := 0
	_, err = backoff.Retry(ctx, func() (struct{}, error) {
		if attempts > ep.Retries {
			return struct{}{}, backoff.Permanent(
				fmt.Errorf("webhook: max retries (%d) exceeded", ep.Retries))
		}
		attempts++

		resp, err := client.Do(req)
		if err != nil {
			return struct{}{}, fmt.Errorf("webhook: send: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return struct{}{}, fmt.Errorf("webhook: endpoint returned %d", resp.StatusCode)
		}
		return struct{}{}, nil
	}, backoff.WithBackOff(bo))
	if err != nil {
		w.logger.Error("webhook: delivery failed after retries",
			"error", err, "endpoint", ep.URL, "event_id", event.ID)
		return
	}

	w.logger.Debug("webhook: delivered", "endpoint", ep.URL, "event_id", event.ID)
}

// signPayload computes HMAC-SHA256(secret, timestamp + "." + hex(payload)).
// The timestamp prevents replay: receivers reject signatures older than a
// configurable tolerance window.
func signPayload(secret string, timestamp int64, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(fmt.Sprintf("%d.%s", timestamp, hex.EncodeToString(payload))))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature validates a webhook received from PulseFlow. This is
// provided for testing and downstream consumers.
func VerifySignature(secret string, timestamp int64, payload []byte, signature string) bool {
	expected := signPayload(secret, timestamp, payload)
	return hmac.Equal([]byte(expected), []byte(signature))
}
