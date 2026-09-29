package usecase_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/redis"
	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/tracing"
	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/webhook"
	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/alicebob/miniredis/v2"
	rd "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock implementations ---

type mockDestRepo struct {
	disabledIDs []string
}

func (m *mockDestRepo) ListActive(ctx context.Context, eventType entity.EventType, source string) ([]*entity.Destination, error) {
	return nil, nil
}
func (m *mockDestRepo) GetDestination(ctx context.Context, id string) (*entity.Destination, error) {
	return nil, nil
}
func (m *mockDestRepo) DisableDestination(ctx context.Context, id string) error {
	m.disabledIDs = append(m.disabledIDs, id)
	return nil
}

func createTestEvent(eventType string, data map[string]any) *entity.Event {
	payload, _ := json.Marshal(data)
	return &entity.Event{
		ID:       entity.EventID("01HZTEST000000000000000000"),
		Type:     entity.EventType(eventType),
		Source:   "billing",
		Subject:  "order:123",
		Data:     payload,
		Version:  "v1",
		Metadata: map[string]string{},
	}
}

func TestWebhookDelivery_E2E_SignatureVerification(t *testing.T) {
	var receivedSig string
	var receivedTimestamp string
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig = r.Header.Get("X-PulseFlow-Signature")
		receivedTimestamp = r.Header.Get("X-PulseFlow-Timestamp")
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	destRepo := &mockDestRepo{}
	sender := webhook.NewSender(destRepo, webhook.Config{
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	}, nil)

	dest := &entity.Destination{
		ID:            "dest-sig-e2e",
		URL:           server.URL,
		PrimarySecret: "e2e-secret",
		Status:        entity.DestinationActive,
	}

	event := createTestEvent("order.created", map[string]any{"amount": 150.0})
	sender.Deliver(context.Background(), dest, event)

	time.Sleep(500 * time.Millisecond)

	// Verify the signature header was sent.
	require.Contains(t, receivedSig, "v1=")

	// Extract the hash from "v1=<hash>".
	hash := receivedSig[3:]

	// Parse timestamp.
	var ts int64
	for _, c := range receivedTimestamp {
		if c >= '0' && c <= '9' {
			ts = ts*10 + int64(c-'0')
		}
	}

	// Verify signature matches.
	valid := webhook.VerifySignature("e2e-secret", ts, receivedBody, hash)
	assert.True(t, valid, "HMAC-SHA256 signature should be valid")
}

func TestWebhookDelivery_E2E_DualSecretSigning(t *testing.T) {
	var receivedSig string
	var receivedBody []byte
	var receivedTimestamp string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig = r.Header.Get("X-PulseFlow-Signature")
		receivedBody, _ = io.ReadAll(r.Body)
		receivedTimestamp = r.Header.Get("X-PulseFlow-Timestamp")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	destRepo := &mockDestRepo{}
	sender := webhook.NewSender(destRepo, webhook.Config{
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	}, nil)

	dest := &entity.Destination{
		ID:              "dest-dual-e2e",
		URL:             server.URL,
		PrimarySecret:   "primary-secret",
		SecondarySecret: "secondary-secret",
		RotationExpiresAt: time.Now().Add(1 * time.Hour).UTC(),
		Status:          entity.DestinationActive,
	}

	event := createTestEvent("order.created", map[string]any{"amount": 200.0})
	sender.Deliver(context.Background(), dest, event)
	time.Sleep(500 * time.Millisecond)

	// Dual-secret signing produces "v1=<hash1>,v1=<hash2>".
	require.Contains(t, receivedSig, "v1=")
	require.Contains(t, receivedSig, ",v1=", "dual-secret header should contain comma-separated v1 entries")

	// Verify both signatures are valid.
	parts := splitAndExtractSignatures(receivedSig)
	require.Len(t, parts, 2)

	// Parse timestamp.
	var ts int64
	for _, c := range receivedTimestamp {
		if c >= '0' && c <= '9' {
			ts = ts*10 + int64(c-'0')
		}
	}

	assert.True(t, webhook.VerifySignature("primary-secret", ts, receivedBody, parts[0]),
		"primary signature should be valid")
	assert.True(t, webhook.VerifySignature("secondary-secret", ts, receivedBody, parts[1]),
		"secondary signature should be valid")
}

func TestWebhookDelivery_E2E_SingleSecret(t *testing.T) {
	var receivedSig string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig = r.Header.Get("X-PulseFlow-Signature")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	destRepo := &mockDestRepo{}
	sender := webhook.NewSender(destRepo, webhook.Config{
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	}, nil)

	dest := &entity.Destination{
		ID:            "dest-single-e2e",
		URL:           server.URL,
		PrimarySecret: "single-secret",
		Status:        entity.DestinationActive,
	}

	event := createTestEvent("order.created", map[string]any{"amount": 100.0})
	sender.Deliver(context.Background(), dest, event)
	time.Sleep(500 * time.Millisecond)

	// Single-secret signing produces "v1=<hash>" (no comma).
	require.Contains(t, receivedSig, "v1=")
	assert.NotContains(t, receivedSig, ",v1=", "single-secret header should not contain comma")
}

func TestWebhookDelivery_E2E_410AutoDisable(t *testing.T) {
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusGone)
	}))
	defer server.Close()

	destRepo := &mockDestRepo{}
	sender := webhook.NewSender(destRepo, webhook.Config{
		Timeout:    5 * time.Second,
		MaxRetries: 1,
		RetryDelay: 10 * time.Millisecond,
	}, nil)

	dest := &entity.Destination{
		ID:            "dest-410-e2e",
		URL:           server.URL,
		PrimarySecret: "secret",
		Status:        entity.DestinationActive,
	}

	event := createTestEvent("order.created", map[string]any{"amount": 100.0})
	sender.Deliver(context.Background(), dest, event)
	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, int32(1), atomic.LoadInt32(&requestCount), "should only attempt once on 410")
	require.Len(t, destRepo.disabledIDs, 1)
	assert.Equal(t, "dest-410-e2e", destRepo.disabledIDs[0])
}

func TestWebhookDelivery_E2E_RetryOn5xx(t *testing.T) {
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

	destRepo := &mockDestRepo{}
	sender := webhook.NewSender(destRepo, webhook.Config{
		Timeout:    5 * time.Second,
		MaxRetries: 5,
		RetryDelay: 10 * time.Millisecond,
	}, nil)

	dest := &entity.Destination{
		ID:            "dest-retry-e2e",
		URL:           server.URL,
		PrimarySecret: "secret",
		Status:        entity.DestinationActive,
	}

	event := createTestEvent("order.created", map[string]any{"amount": 100.0})
	sender.Deliver(context.Background(), dest, event)
	time.Sleep(1 * time.Second)

	assert.GreaterOrEqual(t, atomic.LoadInt32(&requestCount), int32(3), "should retry on 5xx until success")
}

func TestConcurrencyLimiter_E2E_Limits(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient := rd.NewClient(&rd.Options{Addr: mr.Addr()})
	limiter := redis.NewConcurrencyLimiter(redisClient, nil, "concurrency", 60*time.Second)

	ctx := context.Background()
	limit := 3

	// Acquire up to limit.
	for i := 0; i < limit; i++ {
		ok, err := limiter.Acquire(ctx, "dest-e2e-lim", limit)
		require.NoError(t, err)
		assert.True(t, ok)
	}

	// Exceeding limit should fail.
	ok, err := limiter.Acquire(ctx, "dest-e2e-lim", limit)
	require.NoError(t, err)
	assert.False(t, ok)

	// Release one.
	err = limiter.Release(ctx, "dest-e2e-lim")
	require.NoError(t, err)

	// Should be able to acquire again.
	ok, err = limiter.Acquire(ctx, "dest-e2e-lim", limit)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestTracing_E2E_ContextPropagation(t *testing.T) {
	p := tracing.NewOTelPropagator()

	// Simulate ingestion: extract W3C traceparent from HTTP headers.
	traceID := "0123456789abcdef0123456789abcdef"
	spanID := "0123456789abcdef"
	header := http.Header{}
	header.Set("Traceparent", "00-"+traceID+"-"+spanID+"-01")

	ctx, span := p.ExtractHTTP(context.Background(), header)
	require.True(t, span.SpanContext().IsValid())

	// Inject into event metadata for Redis stream propagation.
	metadata := make(map[string]string)
	p.ToMetadata(ctx, metadata)

	assert.Equal(t, traceID, metadata[entity.TraceIDKey])
	assert.Equal(t, spanID, metadata[entity.SpanIDKey])

	// Consumer side: extract from metadata.
	ctx2, span2 := p.FromMetadata(context.Background(), metadata)
	require.True(t, span2.SpanContext().IsValid())
	assert.Equal(t, traceID, span2.SpanContext().TraceID().String())

	// TraceIDFromContext should return the trace ID.
	traceIDResult := entity.TraceIDFromContext(ctx2)
	assert.Equal(t, traceID, traceIDResult)
}

func TestTracing_E2E_HTTPHeaderRoundtrip(t *testing.T) {
	p := tracing.NewOTelPropagator()

	// Extract from incoming headers, then inject into outgoing headers.
	header1 := http.Header{}
	header1.Set("Traceparent", "00-abcdef0123456789abcdef0123456789-abcdef0123456789-01")

	ctx, _ := p.ExtractHTTP(context.Background(), header1)

	// Inject into outgoing headers.
	header2 := http.Header{}
	p.InjectHTTP(ctx, header2)

	traceparent2 := header2.Get("Traceparent")
	assert.NotEmpty(t, traceparent2, "traceparent should be injected")
	assert.Contains(t, traceparent2, "00-")
}

// --- Helpers ---

func splitAndExtractSignatures(header string) []string {
	var result []string
	current := ""
	for _, c := range header {
		if c == ',' {
			if len(current) > 3 && current[:3] == "v1=" {
				result = append(result, current[3:])
			}
			current = ""
		} else {
			current += string(c)
		}
	}
	if len(current) > 3 && current[:3] == "v1=" {
		result = append(result, current[3:])
	}
	return result
}
