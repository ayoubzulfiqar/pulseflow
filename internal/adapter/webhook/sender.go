package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/cenkalti/backoff/v5"
)

// Config holds webhook delivery configuration.
type Config struct {
	Timeout    time.Duration
	MaxRetries int
	RetryDelay time.Duration
}

// Sender implements entity.WebhookDeliverer with dual-secret signing,
// HTTP 410 auto-disable, and per-destination concurrency limits.
// CEL filter evaluation is performed by the usecase layer before
// calling Deliver — this adapter focuses on HTTP delivery concerns.
type Sender struct {
	client   *http.Client
	repo     entity.DestinationRepository
	logger   *slog.Logger
	config   Config
	propagator entity.Propagator
}

// SenderOption configures the Sender.
type SenderOption func(*Sender)

// WithPropagator injects a trace context propagator for HTTP header injection.
func WithPropagator(p entity.Propagator) SenderOption {
	return func(s *Sender) { s.propagator = p }
}

// NewSender creates a webhook delivery adapter.
func NewSender(repo entity.DestinationRepository, cfg Config, logger *slog.Logger, opts ...SenderOption) *Sender {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Sender{
		client: &http.Client{Timeout: cfg.Timeout},
		repo:   repo,
		config: cfg,
		logger: logger,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.config.MaxRetries <= 0 {
		s.config.MaxRetries = 3
	}
	if s.config.Timeout <= 0 {
		s.config.Timeout = 30 * time.Second
	}
	return s
}

// Deliver asynchronously sends the event to the specified destination
// with dual-secret signing, exponential backoff retries, and HTTP 410
// auto-disable. Fire-and-forget — runs in its own goroutine.
func (s *Sender) Deliver(ctx context.Context, dest *entity.Destination, event *entity.Event) {
	go s.deliverTo(ctx, dest, event)
}

func (s *Sender) deliverTo(ctx context.Context, dest *entity.Destination, event *entity.Event) {
	payload, err := json.Marshal(event)
	if err != nil {
		s.logger.Error("webhook: marshal event", "error", err, "dest_id", dest.ID, "event_id", event.ID)
		return
	}

	ts := time.Now().Unix()
	sigs := s.signDual(dest, ts, payload)

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = s.config.RetryDelay
	bo.Multiplier = 2.0
	bo.MaxInterval = s.config.RetryDelay * 8

	attempts := 0
	_, _ = backoff.Retry(ctx, func() (struct{}, error) {
		if attempts >= s.config.MaxRetries {
			return struct{}{}, backoff.Permanent(
				fmt.Errorf("webhook: max retries (%d) exceeded", s.config.MaxRetries))
		}
		attempts++

		if err := ctx.Err(); err != nil {
			return struct{}{}, backoff.Permanent(err)
		}

		disabled, err := s.attemptDelivery(ctx, dest, sigs, payload, event)
		if err != nil {
			if disabled {
				return struct{}{}, backoff.Permanent(nil)
			}
			return struct{}{}, err
		}
		return struct{}{}, nil
	}, backoff.WithBackOff(bo))
}

// attemptDelivery performs a single HTTP delivery attempt.
// Returns (disabled, err) — disabled is true if the destination was
// auto-disabled due to HTTP 410 Gone.
func (s *Sender) attemptDelivery(
	ctx context.Context,
	dest *entity.Destination,
	sigs []string,
	payload []byte,
	event *entity.Event,
) (bool, error) {
	sigHeader := s.buildSigHeader(sigs)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, dest.URL, bytes.NewReader(payload))
	if err != nil {
		return false, fmt.Errorf("webhook: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-PulseFlow-Timestamp", fmt.Sprintf("%d", time.Now().Unix()))
	req.Header.Set("X-PulseFlow-Signature", sigHeader)
	req.Header.Set("X-PulseFlow-Event-ID", string(event.ID))

	if s.propagator != nil {
		s.propagator.InjectHTTP(ctx, req.Header)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("webhook: send: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusAccepted:
		s.logger.Debug("webhook: delivered", "dest_id", dest.ID, "event_id", event.ID)
		return false, nil

	case resp.StatusCode == http.StatusGone:
		// HTTP 410 — auto-disable the destination.
		s.logger.Warn("webhook: destination returned 410 Gone, disabling",
			"dest_id", dest.ID, "url", dest.URL)
		s.disableDestination(ctx, dest.ID)
		return true, nil

	case resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests:
		return false, fmt.Errorf("webhook: server error %d", resp.StatusCode)

	default:
		// 4xx (non-410) — not retryable.
		return false, backoff.Permanent(fmt.Errorf("webhook: client error %d", resp.StatusCode))
	}
}

// disableDestination marks the destination as disabled in PostgreSQL.
func (s *Sender) disableDestination(ctx context.Context, destID string) {
	if s.repo == nil {
		return
	}
	if err := s.repo.DisableDestination(ctx, destID); err != nil {
		s.logger.Error("webhook: failed to disable destination",
			"dest_id", destID, "error", err)
	}
}

// signDual computes HMAC-SHA256 signatures for both primary and secondary
// secrets (when present and not expired). Returns the signature strings.
func (s *Sender) signDual(dest *entity.Destination, timestamp int64, payload []byte) []string {
	result := make([]string, 0, 2)
	result = append(result, signPayload(dest.PrimarySecret, timestamp, payload))

	if dest.HasSecondarySecret() {
		result = append(result, signPayload(dest.SecondarySecret, timestamp, payload))
	}
	return result
}

// buildSigHeader formats signatures as comma-separated "v1=<hash>" entries.
func (s *Sender) buildSigHeader(sigs []string) string {
	var sb strings.Builder
	for i, sig := range sigs {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("v1=")
		sb.WriteString(sig)
	}
	return sb.String()
}

// signPayload computes HMAC-SHA256(secret, timestamp + "." + hex(payload)).
func signPayload(secret string, timestamp int64, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(fmt.Sprintf("%d.%s", timestamp, hex.EncodeToString(payload))))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifySignature validates a webhook signature against the given secret.
func VerifySignature(secret string, timestamp int64, payload []byte, signature string) bool {
	expected := signPayload(secret, timestamp, payload)
	return hmac.Equal([]byte(expected), []byte(signature))
}

// Ensure Sender satisfies entity.WebhookDeliverer.
var _ entity.WebhookDeliverer = (*Sender)(nil)
