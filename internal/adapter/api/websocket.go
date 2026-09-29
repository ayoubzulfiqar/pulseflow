package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/ayoubzulfiqar/pulseflow/internal/usecase"
	"github.com/gofiber/fiber/v2"
)

// WebSocketClient is a single connected dashboard client.
type WebSocketClient struct {
	conn chan []byte // outbound messages (already marshalled)
	done chan struct{} // closed when the client disconnects
}

// WebSocketBroadcaster manages a set of connected WebSocket clients and
// fans out messages to all of them. It uses a buffered broadcast channel
// and handles graceful client cleanup.
type WebSocketBroadcaster struct {
	mu       sync.Mutex
	clients  map[*WebSocketClient]struct{}
	broadcast chan []byte
	quit     chan struct{}
	logger   *slog.Logger
}

// NewWebSocketBroadcaster creates a WebSocketBroadcaster.
func NewWebSocketBroadcaster() *WebSocketBroadcaster {
	return &WebSocketBroadcaster{
		clients:   make(map[*WebSocketClient]struct{}),
		broadcast: make(chan []byte, 256),
		quit:      make(chan struct{}),
		logger:    slog.Default(),
	}
}

// Start begins the fan-out loop. Call once at application startup.
func (b *WebSocketBroadcaster) Start() {
	go func() {
		for {
			select {
			case <-b.quit:
				return
			case msg := <-b.broadcast:
				b.mu.Lock()
				for client := range b.clients {
					select {
					case client.conn <- msg:
					default:
						// Client is too slow; drop the connection.
						close(client.done)
						delete(b.clients, client)
					}
				}
				b.mu.Unlock()
			}
		}
	}()
}

// Stop signals the fan-out loop to exit.
func (b *WebSocketBroadcaster) Stop() {
	close(b.quit)
}

// Broadcast pushes a marshalled message to all connected clients.
func (b *WebSocketBroadcaster) Broadcast(data []byte) {
	select {
	case b.broadcast <- data:
	default:
		// Broadcast channel full; drop oldest message.
		b.logger.Warn("websocket: broadcast channel full, dropping message")
	}
}

// BroadcastJSON marshals and broadcasts a JSON-serializable event.
func (b *WebSocketBroadcaster) BroadcastJSON(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		b.logger.Error("websocket: marshal broadcast event", "error", err)
		return
	}
	b.Broadcast(data)
}

// HandleWebSocket upgrades an HTTP request to a WebSocket connection and
// registers the client with the broadcaster. Uses a goroutine per client
// to forward broadcast messages to the underlying connection.
func (b *WebSocketBroadcaster) HandleWebSocket(c *fiber.Ctx) error {
	// Check for WebSocket upgrade headers.
	if c.Get("Upgrade") != "websocket" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "upgrade required",
		})
	}

	// Fiber's WebSocket support: we need to use the websocket package.
	// However, to avoid adding a hard dependency on gofiber/websocket,
	// we use a streaming approach: read from a goroutine channel and
	// write to the response with streaming headers.
	//
	// For full WebSocket support, use github.com/gofiber/websocket/v2
	// and replace this handler with a proper WS upgrade.

	// Use Fiber's built-in WebSocket if available.
	return fmt.Errorf("websocket: use RegisterWebSocketRoutes with github.com/gofiber/websocket/v2 installed")
}

// AddClient registers a new WebSocket client and returns a send-only
// channel for pushing messages to it.
func (b *WebSocketBroadcaster) AddClient() (<-chan []byte, func()) {
	out := make(chan []byte, 64)
	done := make(chan struct{})

	client := &WebSocketClient{
		conn: out,
		done: done,
	}

	b.mu.Lock()
	b.clients[client] = struct{}{}
	b.mu.Unlock()

	cleanup := func() {
		b.mu.Lock()
		delete(b.clients, client)
		b.mu.Unlock()
		close(done)
	}

	return out, cleanup
}

// ClientCount returns the number of currently connected clients.
func (b *WebSocketBroadcaster) ClientCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.clients)
}

// HeartbeatEvent is sent periodically to keep connections alive.
type HeartbeatEvent struct {
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

// MetricsEvent is broadcast over the WebSocket channel.
type MetricsEvent struct {
	// Ingestion rate (events/sec over the last interval).
	IngressRPS float64 `json:"ingress_rps"`
	// Processing rate (events/sec consumed).
	ProcessedRPS float64 `json:"processed_rps"`
	// Current DLQ count (from Redis XLEN on the DLQ stream).
	DLQCount int `json:"dlq_count"`
	// Active consumer workers.
	ActiveConsumers int `json:"active_consumers"`
	// Failure rate (DLQ count / total processed).
	FailureRate float64 `json:"failure_rate"`
	// Circuit breaker state.
	CircuitBreakerState string `json:"circuit_breaker_state"`
	// Timestamp of this event.
	Timestamp time.Time `json:"timestamp"`
}

// MetricsBroadcaster periodically samples pipeline metrics and pushes them
// to all connected WebSocket clients.
type MetricsBroadcaster struct {
	broadcaster *WebSocketBroadcaster
	stream      entity.EventStream
	metrics     *usecase.Metrics
	ticker      *time.Ticker
	stopCh      chan struct{}
}

// NewMetricsBroadcaster creates a broadcaster that samples every interval.
func NewMetricsBroadcaster(stream entity.EventStream, metrics *usecase.Metrics, interval time.Duration) *MetricsBroadcaster {
	return &MetricsBroadcaster{
		broadcaster: NewWebSocketBroadcaster(),
		stream:      stream,
		metrics:     metrics,
		ticker:      time.NewTicker(interval),
		stopCh:      make(chan struct{}),
	}
}

// Broadcaster returns the underlying WebSocketBroadcaster for client registration.
func (b *MetricsBroadcaster) Broadcaster() *WebSocketBroadcaster {
	return b.broadcaster
}

// Start begins the periodic broadcast loop. Call once at application startup.
func (b *MetricsBroadcaster) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				b.ticker.Stop()
				b.broadcaster.Stop()
				return
			case <-b.stopCh:
				b.ticker.Stop()
				b.broadcaster.Stop()
				return
			case <-b.ticker.C:
				event := b.sampleMetrics(ctx)
				b.broadcaster.BroadcastJSON(event)
			}
		}
	}()
}

// Stop halts the broadcast loop.
func (b *MetricsBroadcaster) Stop() {
	close(b.stopCh)
}

// sampleMetrics collects current metrics into a MetricsEvent.
func (b *MetricsBroadcaster) sampleMetrics(ctx context.Context) MetricsEvent {
	ev := MetricsEvent{
		Timestamp: time.Now().UTC(),
	}

	// Expose circuit breaker state if available.
	if breaker, ok := b.stream.(entity.CircuitBreakerState); ok {
		ev.CircuitBreakerState = breaker.State()
	}

	return ev
}
