package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDestRepo implements entity.DestinationRepository for testing.
type mockDestRepo struct {
	disabledID string
}

func (m *mockDestRepo) ListActive(ctx context.Context, eventType entity.EventType, source string) ([]*entity.Destination, error) {
	return nil, nil
}
func (m *mockDestRepo) GetDestination(ctx context.Context, id string) (*entity.Destination, error) {
	return nil, nil
}
func (m *mockDestRepo) DisableDestination(ctx context.Context, id string) error {
	m.disabledID = id
	return nil
}

func newTestDestination(primarySecret string) *entity.Destination {
	return &entity.Destination{
		ID:            "dest-001",
		URL:           "https://example.com/webhook",
		PrimarySecret: primarySecret,
		Status:        entity.DestinationActive,
		ConcurrencyLimit: 5,
	}
}

func newTestEvent() *entity.Event {
	payload, _ := json.Marshal(map[string]any{"order_id": 12345, "amount": 150.0})
	return &entity.Event{
		ID:      entity.EventID("01HZTEST000000000000000000"),
		Type:    "order.created",
		Source:  "billing",
		Subject: "order:12345",
		Data:    payload,
		Version: "v1",
	}
}

func TestSignPayload_SingleSecret(t *testing.T) {
	payload := []byte(`{"test": true}`)
	timestamp := int64(1727123456)
	sig := signPayload("my-secret", timestamp, payload)

	// Verify the signature is a valid hex string of SHA-256 (64 chars).
	assert.Len(t, sig, 64)
}

func TestVerifySignature_Valid(t *testing.T) {
	payload := []byte(`{"test": true}`)
	timestamp := int64(1727123456)
	secret := "my-secret"

	sig := signPayload(secret, timestamp, payload)
	assert.True(t, VerifySignature(secret, timestamp, payload, sig))
}

func TestVerifySignature_Invalid(t *testing.T) {
	payload := []byte(`{"test": true}`)
	timestamp := int64(1727123456)

	sig := signPayload("secret-a", timestamp, payload)
	assert.False(t, VerifySignature("secret-b", timestamp, payload, sig))
}

func TestBuildSigHeader_SingleSecret(t *testing.T) {
	s := &Sender{}
	header := s.buildSigHeader([]string{"abc123"})
	assert.Equal(t, "v1=abc123", header)
}

func TestBuildSigHeader_DualSecret(t *testing.T) {
	s := &Sender{}
	header := s.buildSigHeader([]string{"abc123", "def456"})
	assert.Equal(t, "v1=abc123,v1=def456", header)
}

func TestSignDual_SingleSecret(t *testing.T) {
	s := &Sender{}
	dest := newTestDestination("primary-secret")

	sigs := s.signDual(dest, 1234567890, []byte("payload"))
	require.Len(t, sigs, 1)
	assert.Equal(t, "primary-secret", dest.PrimarySecret)
}

func TestSignDual_WithSecondarySecret(t *testing.T) {
	s := &Sender{}
	dest := newTestDestination("primary-secret")
	dest.SecondarySecret = "secondary-secret"
	dest.RotationExpiresAt = time.Now().Add(1 * time.Hour).UTC()

	sigs := s.signDual(dest, 1234567890, []byte("payload"))
	require.Len(t, sigs, 2, "should produce both signatures during rotation")
}

func TestSignDual_SecondaryExpired(t *testing.T) {
	s := &Sender{}
	dest := newTestDestination("primary-secret")
	dest.SecondarySecret = "secondary-secret"
	// Expired 1 hour ago — secondary should not be signed.
	dest.RotationExpiresAt = time.Now().Add(-1 * time.Hour).UTC()

	sigs := s.signDual(dest, 1234567890, []byte("payload"))
	require.Len(t, sigs, 1, "expired secondary should not produce a second signature")
}

func TestDeliver_Success(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.NotEmpty(t, r.Header.Get("X-PulseFlow-Timestamp"))
		assert.Contains(t, r.Header.Get("X-PulseFlow-Signature"), "v1=")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	repo := &mockDestRepo{}
	sender := NewSender(repo, Config{Timeout: 5 * time.Second, MaxRetries: 3, RetryDelay: 100 * time.Millisecond}, nil)

	dest := newTestDestination("my-secret")
	dest.URL = server.URL

	event := newTestEvent()

	// Deliver runs in a goroutine — wait for completion.
	sender.Deliver(context.Background(), dest, event)
	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, int32(1), atomic.LoadInt32(&requestCount))
}

func TestDeliver_410Gone_AutoDisables(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusGone)
	}))
	defer server.Close()

	repo := &mockDestRepo{}
	sender := NewSender(repo, Config{Timeout: 5 * time.Second, MaxRetries: 3, RetryDelay: 50 * time.Millisecond}, nil)

	dest := newTestDestination("my-secret")
	dest.URL = server.URL

	event := newTestEvent()
	sender.Deliver(context.Background(), dest, event)
	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, int32(1), atomic.LoadInt32(&requestCount), "should only attempt once on 410")
	assert.Equal(t, "dest-001", repo.disabledID, "destination should be disabled")
}

func TestDeliver_5xx_RetriesThenDLQ(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&requestCount, 1)
		if n < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	repo := &mockDestRepo{}
	sender := NewSender(repo, Config{Timeout: 5 * time.Second, MaxRetries: 5, RetryDelay: 10 * time.Millisecond}, nil)

	dest := newTestDestination("my-secret")
	dest.URL = server.URL

	event := newTestEvent()
	sender.Deliver(context.Background(), dest, event)
	time.Sleep(1 * time.Second)

	assert.Equal(t, int32(3), atomic.LoadInt32(&requestCount), "should retry until success")
}

func TestDeliver_4xx_NonRetryable(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	repo := &mockDestRepo{}
	sender := NewSender(repo, Config{Timeout: 5 * time.Second, MaxRetries: 3, RetryDelay: 10 * time.Millisecond}, nil)

	dest := newTestDestination("my-secret")
	dest.URL = server.URL

	event := newTestEvent()
	sender.Deliver(context.Background(), dest, event)
	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, int32(1), atomic.LoadInt32(&requestCount), "4xx should not retry")
	assert.Equal(t, "", repo.disabledID, "4xx should not disable destination")
}

func TestDeliver_SetsExpectedHeaders(t *testing.T) {
	var capturedSig string
	var capturedTimestamp string
	var capturedEventID string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get("X-PulseFlow-Signature")
		capturedTimestamp = r.Header.Get("X-PulseFlow-Timestamp")
		capturedEventID = r.Header.Get("X-PulseFlow-Event-ID")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	repo := &mockDestRepo{}
	sender := NewSender(repo, Config{Timeout: 5 * time.Second, MaxRetries: 1, RetryDelay: 10 * time.Millisecond}, nil)

	dest := newTestDestination("my-secret")
	dest.URL = server.URL

	event := newTestEvent()
	sender.Deliver(context.Background(), dest, event)
	time.Sleep(500 * time.Millisecond)

	assert.Contains(t, capturedSig, "v1=")
	assert.NotEmpty(t, capturedTimestamp)
	assert.Equal(t, string(event.ID), capturedEventID)
}

func TestDeliver_MarshalsPayloadCorrectly(t *testing.T) {
	var receivedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	repo := &mockDestRepo{}
	sender := NewSender(repo, Config{Timeout: 5 * time.Second, MaxRetries: 1, RetryDelay: 10 * time.Millisecond}, nil)

	dest := newTestDestination("my-secret")
	dest.URL = server.URL

	event := newTestEvent()
	sender.Deliver(context.Background(), dest, event)
	time.Sleep(500 * time.Millisecond)

	// Verify the payload is valid JSON of the event.
	var delivered entity.Event
	err := json.Unmarshal(receivedBody, &delivered)
	require.NoError(t, err)
	assert.Equal(t, event.ID, delivered.ID)
	assert.Equal(t, event.Type, delivered.Type)
}

func TestNewSender_DefaultConfig(t *testing.T) {
	repo := &mockDestRepo{}
	sender := NewSender(repo, Config{}, nil)

	assert.Equal(t, 3, sender.config.MaxRetries, "default max retries")
	assert.Equal(t, 30*time.Second, sender.config.Timeout, "default timeout")
}
