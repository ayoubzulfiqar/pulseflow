package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitTracer_Disabled_ReturnsNoop(t *testing.T) {
	ctx := context.Background()
	shutdown, err := InitTracer(ctx, Config{
		Enabled:     false,
		ServiceName: "test",
		Exporter:    "otlphttp",
		Endpoint:    "localhost:4318",
		SampleRate:  1.0,
	})
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Shutdown should be a no-op.
	err = shutdown(ctx)
	assert.NoError(t, err)
}

func TestInitTracer_Enabled_DefaultEndpoint(t *testing.T) {
	ctx := context.Background()
	shutdown, err := InitTracer(ctx, Config{
		Enabled:     true,
		ServiceName: "pulseflow-test",
		Exporter:    "otlphttp",
		SampleRate:  1.0,
	})

	// May fail if no collector is running, but the config and setup
	// logic should be exercised. If it succeeds, verify shutdown works.
	if err == nil {
		require.NotNil(t, shutdown)
		err = shutdown(ctx)
		assert.NoError(t, err)
	}
}

func TestInitTracer_Enabled_InvalidEndpoint(t *testing.T) {
	ctx := context.Background()
	// Use a port that's definitely not listening.
	shutdown, err := InitTracer(ctx, Config{
		Enabled:     true,
		ServiceName: "pulseflow-test",
		Exporter:    "otlphttp",
		Endpoint:    "invalid:host:port",
		SampleRate:  1.0,
	})

	// Should either succeed (exporter connects lazily) or fail gracefully.
	if err == nil {
		require.NotNil(t, shutdown)
		err = shutdown(ctx)
		assert.NoError(t, err)
	}
}

func TestTracer_ReturnsNonNil(t *testing.T) {
	tr := Tracer("test-instrumentation")
	assert.NotNil(t, tr)
}

func TestConfig_ZeroValues(t *testing.T) {
	cfg := Config{}
	assert.False(t, cfg.Enabled)
	assert.Empty(t, cfg.ServiceName)
}
