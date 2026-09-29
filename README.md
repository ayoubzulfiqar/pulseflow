# PulseFlow — Event-Driven Pipeline

Production-grade event ingestion, processing, and delivery platform. Built for high-throughput, fault-tolerant, observability-first event pipelines.

## Overview

PulseFlow is an event-driven pipeline that ingests domain events via HTTP, persists them durably, publishes them to a Redis Streams consumer-group topology for asynchronous processing, and delivers signed webhook notifications to downstream systems. Designed for horizontal scaling, zero-downtime deploys, and graceful degradation under partial infrastructure failure.

**Key differentiators:**
- Events are saved to PostgreSQL and published to Redis Streams before responding — no data loss if the stream is slow
- Dead workers' messages are automatically reclaimed by other consumers via XCLAIM
- Failed messages go to a dead-letter queue (DLQ) with configurable retries (default: 5)
- Replay historical events from PostgreSQL within any time window
- Webhook payloads are signed with HMAC-SHA256 and include a timestamp to prevent replay attacks
- Rotate webhook signing keys without downtime using dual-secret mode
- Filter which events each destination receives using CEL expressions (Common Expression Language)
- Each destination has a configurable concurrency limit. Redis-backed semaphores prevent overwhelming downstream services
- If a destination returns HTTP 410 Gone, it is automatically disabled in PostgreSQL
- Circuit breaker protects against Redis failures
- Each tenant gets its own API keys, rate limits, and audit trail
- PostgreSQL partitions event data by month for fast reads and easy cleanup
- OpenTelemetry tracing shows the full path of every event across HTTP and Redis Streams
- Prometheus metrics, JSON logs, and a real-time WebSocket metrics stream
- API for managing DLQ, replaying events, and resetting circuit breakers

## Control Plane API

The control plane provides operator-level APIs for managing and observing the event pipeline. All endpoints require API key authentication when `admin.auth_required: true` is set in configuration.

### DLQ Management

#### GET /v1/admin/dlq

List all dead-lettered messages with server-side filtering and pagination.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | int | `100` | Max results (capped at 1000) |
| `offset` | int | `0` | Skip first N results |
| `types` | string | — | Comma-separated event types |
| `sources` | string | — | Comma-separated event sources |
| `status` | string | — | Filter by DLQ status: `pending`, `processing`, `locked`, `resolved` |
| `min_retry` | int | — | Minimum retry count threshold |

**Response (200):**
```json
{
  "data": [{
    "id": "1727123456789-0",
    "event": { "id": "01HZ...", "type": "user.created", "source": "api", "data": {} },
    "reason": "retry 3: connection timeout",
    "retry_count": 3,
    "failed_at": "2024-09-28T12:00:00Z",
    "consumer": "worker-3",
    "status": "pending"
  }],
  "count": 1,
  "offset": 0,
  "limit": 100
}
```

The DLQ is mirrored to PostgreSQL (`dlq_messages` table) for durable audit and querying independent of Redis stream retention.

#### POST /v1/admin/dlq/retry

Re-enqueue selected DLQ messages back to the main stream (`pulseflow:events`) with reset retry counters.

**Request body:**
```json
{ "ids": ["1727123456789-0", "1727123456790-0"] }
```

**Response (200):**
```json
{ "requested": 2, "requeued": 2, "skipped": 0, "failed": [] }
```

#### DELETE /v1/admin/dlq/purge?archive=true

Purge all messages from the DLQ stream. When `archive=true` (default), records are persisted to PostgreSQL before deletion for historical audit.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `archive` | bool | `true` | Archive to PostgreSQL before purging |

**Response (200):**
```json
{ "deleted": 42 }
```

### Time-Travel Replay

#### POST /v1/admin/replay

Query historical events from PostgreSQL within a time window and re-publish them to the Redis stream for reprocessing. Events are not re-inserted into PostgreSQL — only republished to the stream.

**Request body:**
```json
{
  "from": "2024-09-01T00:00:00Z",
  "to": "2024-09-28T23:59:59Z",
  "types": ["user.created", "order.placed"],
  "sources": ["billing-service"],
  "subjects": ["user:123"],
  "max_events": 5000
}
```

**Response (200):**
```json
{ "replayed": 5000, "successful": 5000, "failed": 0, "errors": [] }
```

Each replayed event is enriched with `replayed_at` and `replay_origin: time_travel` metadata for downstream processors.

### Circuit Breaker Control

#### GET /v1/admin/circuit-breaker

Returns the current state of the Redis-stream circuit breaker.

```json
{ "name": "redis-stream", "state": "closed", "failing": 0, "total_calls": 1234 }
```

#### POST /v1/admin/circuit-breaker/reset

Manually reset the circuit breaker. Note: sony/gobreaker does not expose a manual reset API; the breaker transitions to half-open automatically after the configured `reset_timeout`. This endpoint logs the reset request.

### WebSocket Metrics Stream

#### GET /v1/ws/metrics

A WebSocket endpoint that streams real-time pipeline metrics to connected dashboard clients. Messages are JSON-encoded `MetricsEvent` objects:

```json
{
  "ingress_rps": 42.5,
  "processed_rps": 41.2,
  "dlq_count": 3,
  "active_consumers": 4,
  "failure_rate": 0.02,
  "circuit_breaker_state": "closed",
  "timestamp": "2024-09-28T12:00:00Z"
}
```

A heartbeat message is sent immediately upon connection. The server samples metrics every 2 seconds and broadcasts to all connected clients. Slow clients are disconnected after 60 seconds of inactivity.

## Multi-Tenancy

PulseFlow supports first-class multi-tenancy with API key authentication and per-tenant rate limiting.

### Tenant & API Key Management

Tenants and API keys are persisted in PostgreSQL (`tenants` and `api_keys` tables). The `api_keys.key` column stores a SHA-256 hash of the key for secure lookup.

Authentication middleware validates the `X-API-Key` header:

```
X-API-Key: pk_live_a1b2c3d4e5f6...
```

When a valid key is found, the tenant ID is attached to the request context via `TenantContextKey`, and per-tenant rate limiting is applied using a Redis-backed token bucket. When auth is not configured, the system falls back to per-IP rate limiting.

| Plan | RPS Limit | Burst |
|------|-----------|-------|
| Free | 10 | 10 |
| Startup | 100 | 20 |
| Business | 500 | 100 |
| Enterprise | 5000 | 500 |

### Configuration for Multi-Tenancy

Enable API key authentication for admin endpoints:

```yaml
admin:
  auth_required: true
```

With this enabled, all `/v1/admin/*` and `/v1/ws/*` endpoints require a valid `X-API-Key` header.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA PLANE                                     │
│                                                                            │
│  HTTP Ingest          Consumer Workers (N×)            Webhook Delivery   │
│  ┌────────┐          ┌──────────────┐ ┌──────────────┐              ┌──────┐│
│  │Fiber   │   Pub    │   Stream     │ │   Stream     │  Process +   │HTTP  ││
│  │API     ├───┬──────┤   Publish    │ │   Consume    ├───┬──────────┤Client││
│  │        │   │      │(XADD)        │ │ (XREADGROUP) │   │  Ack/    │      ││
│  └────────┘   │      └──────┬───────┘ └──────┬───────┘   │  DLQ     └──────┘│
│               │             │                │            │                │
│               │      ┌──────▼──────┐  ┌──────▼──────┐    │                │
│               │      │   Redis     │  │   Event     │    │                │
│               │      │   Stream    │  │ Processor   │    │                │
│               │      │(Consumer Grp)│ │(Store/DB)    │    │                │
│               │      └──────┬──────┘  └──────┬──────┘    │                │
│               │             │                 │           │                │
│               │       ┌─────┴─────┐     ┌─────┴─────┐     │                │
│               └──────▶│  Store    │     │  Claim    │     │                │
│          Query/Read   │(Postgres) │     │ (XCLAIM)  │     │                │
│                       └───────────┘     └───────────┘     │                │
│                                                            │                │
│              ┌────────────────────────────────────────────┐                 │
│              │              CONTROL PLANE                 │                  │
│              │  Prometheus Metrics  Structured Logs        │                 │
│              │  Circuit Breakers    Health Checks         │                 │
│              └────────────────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Design Rationale

| Decision | Rationale | Alternative |
|---|---|---|
| **Redis Streams** over Kafka | Single-binary dependency, in-process consumer groups, no Zookeeper, XCLAIM provides built-in failover | Apache Kafka (external orchestration) |
| **PostgreSQL** over MySQL | Partitioning for audit-scale writes, native JSON indexing, stronger consistency | MySQL (less robust partitioning) |
| **ULID** over UUID | Time-sortable across distributed consumers; 128-bit, no sequence collisions | UUIDv4 (unsortable), UUIDv1 (MAC-exposed) |
| **Circuit breaker** on stream | Isolates Redis failures from HTTP ingestion; half-open probing allows recovery | No breaker (cascade failure) |
| **Dual-write (store + stream)** | PostgreSQL provides durable replay source; stream provides fan-out | Stream-only (data loss on truncation) |
| **HMAC-SHA256** webhook signatures | Prevents tampering; timestamp header enables replay-window rejection | Plain HTTPS only |

### Clean Architecture Boundaries

Dependency rule: every layer depends only on interfaces defined in inner layers. Domain has zero external deps. Adapters implement domain interfaces.

| Layer | Package | Responsibility |
|---|---|---|
| **Presentation** | `cmd/server`, `internal/adapter/api` | HTTP routing, middleware, request/response handling |
| **Application** | `internal/usecase` | Business orchestration, validation, enrichment |
| **Domain** | `internal/entity` | Event/Destination/DLQ entities, ports (Repository, Stream, Propagator, Tracer), validation rules |
| **Infrastructure** | `internal/adapter/{redis,postgres,tracing,filter,webhook}` | Redis Streams, PostgreSQL, OTel tracing, CEL filtering, webhook delivery implementations |
| **Configuration** | `internal/config`, `config.yaml` | Viper-based loader with env override |

## Data Flow

### Ingestion Path

```
Client Request
     │
     ▼
POST /v1/events
     │
     ▼
[api/handler.go] — Ingest handler
  • Parse JSON body → IngestRequest
  • Validate required fields (source, type, subject, data)
  • Generate ULID if not provided
  • Set timestamp & version defaults
     │
     ▼
[usecase/ingest.go] — IngestUseCase
  • Validate event (business rules)
  • Enrich metadata (ingested_at, trace_id)
  • Store to PostgreSQL (write-ahead durability)
  • Publish to Redis Stream (fan-out)
  • Fire webhooks asynchronously (non-blocking)
  • Emit Prometheus counter
     │
     ▼
201 Created ← Event DTO
     │
     ▼
Redis Stream ←─────────────── Consumer Group Workers (N)
     │
     ▼
[XREADGROUP] → [usecase/process.go] — EventProcessor
  • Parse stream message → Event
  • Call EventProcessor.Process() (default: store to Postgres)
  • On success: XACK
  • On failure (retry < MaxDLQRetries): requeue to DLQ
  • On failure (retry >= MaxDLQRetries): lock in DLQ
  • On unmarshal error: DLQ immediately
     │
     ▼
[XCLAIM tick] — Stale message recovery
  • Every claim_min_idle/2: scan pending messages
  • Claim messages idle > claim_min_idle from dead consumers
  • Re-deliver to current consumer
```

### Query Path

```
GET /v1/events?type=...&from=...&to=...&limit=...&offset=...
     │
     ▼
[api/handler.go] — Query handler
  • Parse query parameters → EventFilter
  • Validate pagination bounds
     │
     ▼
[usecase/query.go] — QueryUseCase
  • Query PostgreSQL via EventRepository.Query()
  • Apply time-range, type, source, subject filters
     │
     ▼
200 OK ← []Event (paginated)
```

## Components

### Event

The core domain entity. ULID-based identifier ensures time-sortable ordering across distributed producers and consumers.

| Field | Type | Description |
|-------|------|-------------|
| `ID` | `EventID` (ULID string) | Globally unique, time-sortable event identifier |
| `Type` | `EventType` | Event classification (e.g., `user.created`) |
| `Source` | `string` | Originating service or component |
| `Subject` | `string` | Domain subject identifier (e.g., `user:123`) |
| `Data` | `json.RawMessage` | Arbitrary JSON payload |
| `Metadata` | `map[string]string` | Enrichment context (ingested_at, trace_id, retry_count) |
| `Timestamp` | `time.Time` | Event occurrence time (defaults to server UTC) |
| `Version` | `string` | Schema version (defaults to `v1`) |
| `Status` | `EventStatus` | Lifecycle state |

Validation rules: ID must be a valid ULID; Source and Subject must be non-empty; Data must be non-nil; Type must be non-empty.

### EventRepository (Port)

Persistence interface defined in the domain layer, implemented by the PostgreSQL adapter.

```go
type EventRepository interface {
    Store(ctx context.Context, event *Event) error
    GetByID(ctx context.Context, id EventID) (*Event, error)
    Query(ctx context.Context, filter EventFilter) ([]*Event, error)
    StoreDLQ(ctx context.Context, msg *DLQMessage) error
    QueryDLQ(ctx context.Context, filter DLQFilter) ([]*DLQMessage, error)
    GetDLQByID(ctx context.Context, id string) (*DLQMessage, error)
    UpdateDLQStatus(ctx context.Context, id string, status DLQStatus, retryCount int) error
    DeleteDLQ(ctx context.Context, ids []string) (int, error)
    Health(ctx context.Context) bool
}
```

### EventStream (Port)

Streaming interface defined in the domain layer, implemented by the Redis Streams adapter.

```go
type EventStream interface {
    Publish(ctx context.Context, event *Event) error
    EnsureGroup(ctx context.Context) error
    Consume(ctx context.Context, consumerName string, batchSize int, handler StreamHandler) error
    Ack(ctx context.Context, ids []string) error
    ClaimStaleMessages(ctx context.Context, consumerName string, minIdle string, batchSize int) ([]StreamMessage, error)
    DeadLetterQueue(ctx context.Context, messages []StreamMessage, reason string) error
    ListDLQ(ctx context.Context, limit, offset int) ([]StreamMessage, error)
    RequeueDLQ(ctx context.Context, ids []string) error
    PurgeDLQ(ctx context.Context) (int, error)
}
```

### IngestUseCase

Validates, enriches, persists, and publishes incoming events. Idempotent on caller-supplied ULIDs.

### ProcessUseCase

Runs consumer workers that read from Redis Streams via consumer groups. Implements ack/nack, retry isolation with DLQ (max 5 retries, then permanently locked), XCLAIM failover for stale messages, and graceful shutdown.

### QueryUseCase

Provides paginated, filtered read access to the event store. Supports time-range, type, source, subject filtering, and pagination (limit max 1000, offset).

### DLQUseCase

Dead-letter queue management — list quarantined messages with filtering, bulk re-enqueue to main stream with retry-count reset, purge with optional PostgreSQL archiving. Coordinates between Redis DLQ stream and PostgreSQL audit store.

### EventReplayUseCase

Time-travel replay engine. Queries historical events from PostgreSQL within a time window, enriches with replay metadata, and republishes to the Redis stream for reprocessing without duplicating PostgreSQL records.

### TenantRepository (Port)

Multi-tenant persistence interface for API key validation, tenant lookup (by ID and name), and tenant/key lifecycle management. Implemented by PostgreSQL adapter with SHA-256 key hashing.

### WebSocket Metrics Broadcaster

Periodically samples pipeline metrics (ingress RPS, processing RPS, DLQ count, active consumers, failure rate, circuit breaker state) and broadcasts JSON-encoded events to all connected WebSocket clients over GET /v1/ws/metrics.

### Webhook Delivery

Asynchronous, fire-and-forget webhook delivery with HMAC-SHA256 signing, timestamp anti-replay, exponential backoff retries (cenkalti/backoff/v5), dual-secret rotation support, and OpenTelemetry trace propagation. When a destination returns HTTP 410 Gone, it is immediately disabled in PostgreSQL.

### Destination Entity

Represents a webhook delivery target with dual-secret fields (`PrimarySecret`, `SecondarySecret`), CEL filter expressions, per-URL rate limits (`RateLimitRPS`), concurrency caps (`ConcurrencyLimit`), and lifecycle status (`active`/`disabled`). Rotation is controlled by `RotationExpiresAt` — when set and secondary is present, payloads are dual-signed during the rotation window.

### CEL Filter Engine

Compiles and caches CEL expressions per destination. Evaluates `event.type`, `event.data`, `event.metadata`, and other fields before webhook delivery. Non-matching events are skipped gracefully without consuming retry budget.

### Redis Concurrency Limiter

Distributed semaphore using atomic Lua scripts to enforce per-destination in-flight limits. When saturated, delivery is deferred (not retried) to preserve retry counts. Uses 60s TTL for automatic cleanup of crashed-worker locks.

### Tracing Provider

OpenTelemetry SDK setup package supporting OTLP HTTP and gRPC exporters. Configures W3C TraceContext propagator, batch span processor, and service-name resource attributes. Trace context is injected into Redis Stream messages and extracted by consumer workers for end-to-end span linkage.

### Circuit Breaker

Wraps Redis Streams operations (sony/gobreaker). Trips after 5 consecutive failures, resets after 60s. When open, Publish immediately returns an error without calling Redis, allowing graceful degradation.

## Configuration

Configuration is loaded from `config.yaml` (in CWD or `/etc/pulseflow/config.yaml`) with environment variable overrides using the `PULSEFLOW_` prefix.

### Configuration Reference

| Category | Key | Default | Description |
|---|---|---|---|
| **App** | `app.name` | `pulseflow` | Application name |
| | `app.version` | *(linker-injected)* | Build version |
| | `app.env` | `development` | Environment |
| **Server** | `server.host` | `0.0.0.0` | Bind address |
| | `server.port` | `8080` | Listen port |
| | `server.shutdown_timeout` | `20s` | Graceful shutdown deadline |
| | `server.max_body_bytes` | `10485760` | Max request body (10MB) |
| **Redis** | `redis.addr` | `localhost:6379` | Redis address |
| | `redis.pool_size` | `25` | Connection pool size |
| | `redis.required` | `false` | Fail startup if Redis unreachable |
| **Admin** | `admin.auth_required` | `false` | Require API key auth for admin endpoints |
| **Postgres** | `postgres.dsn` | *(required)* | PostgreSQL connection string |
| | `postgres.max_open_conns` | `25` | Max open connections |
| | `postgres.required` | `false` | Fail startup if Postgres unreachable |
| **Events** | `events.stream_name` | `pulseflow:events` | Redis stream key |
| | `events.group_name` | `pulseflow:workers` | Consumer group |
| | `events.dlq_stream_name` | `pulseflow:dlq` | Dead-letter queue |
| | `events.batch_size` | `100` | Messages per XREADGROUP batch |
| | `events.claim_min_idle` | `60s` | Idle threshold for XCLAIM |
| | `events.consumer_concurrency` | `4` | Worker goroutines |
| **Webhooks** | `webhooks.enabled` | `false` | Enable webhook delivery |
| | `webhooks.endpoints` | `[]` | List of webhook endpoint configs |
| **Rate Limit** | `rate_limit.enabled` | `true` | Per-IP rate limiting |
| | `rate_limit.default_rps` | `200.0` | Requests/sec per IP |
| **Circuit Breaker** | `circuit_breaker.redis.max_failures` | `5` | Failures to trip |
| | `circuit_breaker.redis.reset_timeout` | `60s` | Half-open probe interval |
| **Logging** | `logging.level` | `info` | slog level |
| | `logging.format` | `json` | Log format |
| **Metrics** | `metrics.enabled` | `true` | Expose Prometheus metrics |
| | `metrics.path` | `/metrics` | Metrics endpoint |
| **Tracing** | `tracing.enabled` | `false` | Enable OTLP tracing |
| | `tracing.service_name` | `pulseflow` | Service name for span attribution |
| | `tracing.exporter` | `otlphttp` | Exporter type: `otlphttp` or `otlpgrpc` |
| | `tracing.endpoint` | `localhost:4318` | OTLP collector endpoint |
| | `tracing.sample_rate` | `1.0` | Trace sampling ratio |
| **Webhooks** | `webhooks.enabled` | `false` | Enable webhook delivery |
| | `webhooks.timeout` | `30s` | Default HTTP timeout per delivery |
| | `webhooks.max_retries` | `3` | Max retry attempts before DLQ |
| | `webhooks.retry_delay` | `5s` | Base retry backoff interval |
| | `webhooks.endpoints` | `[]` | List of webhook endpoint configs |

### Environment Variables

All config keys support env overrides with `PULSEFLOW_` prefix:

```bash
PULSEFLOW_SERVER_PORT=9090
PULSEFLOW_REDIS_ADDR=redis:6379
PULSEFLOW_POSTGRES_DSN="postgres://user:***@db:5432/pulseflow?sslmode=disable"
PULSEFLOW_LOGGING_LEVEL=debug
PULSEFLOW_WEBHOOKS_ENABLED=true
PULSEFLOW_TRACING_ENABLED=true
PULSEFLOW_TRACING_EXPORTER=otlphttp
PULSEFLOW_TRACING_ENDPOINT=localhost:4318
```

## API

### POST /v1/events

Ingest a new event. Validated, persisted to PostgreSQL, published to Redis Streams, and triggers async webhook delivery.

**Request body:**
```json
{
  "source": "my-app",
  "type": "user.created",
  "subject": "user:123",
  "data": { "email": "user@example.com" },
  "metadata": { "request_id": "abc-123" },
  "timestamp": "2024-01-15T10:30:00Z",
  "version": "v1"
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `source` | string | no | `"api"` | Event origin |
| `type` | string | yes | — | Event type classification |
| `subject` | string | yes | — | Domain subject identifier |
| `data` | JSON | yes | — | Arbitrary event payload |
| `metadata` | object | no | — | Caller-provided metadata |
| `timestamp` | RFC3339 | no | server UTC | Event occurrence time |
| `version` | string | no | `"v1"` | Schema version |

**Response (201):** Full Event DTO with generated ULID.

### GET /v1/events

Query events with filtering and pagination.

| Parameter | Type | Default | Max | Description |
|-----------|------|---------|-----|-------------|
| `types` | string | — | — | Comma-separated event types |
| `sources` | string | — | — | Comma-separated event sources |
| `subjects` | string | — | — | Comma-separated event subjects |
| `from` | RFC3339 | — | — | Time range start (inclusive) |
| `to` | RFC3339 | — | — | Time range end (inclusive) |
| `limit` | int | `100` | `1000` | Page size |
| `offset` | int | `0` | — | Results to skip |

### GET /v1/events/:id

Retrieve a single event by ULID.

### GET /health

Aggregated health: `200 OK` if all deps healthy, `503` if degraded.

```json
{
  "status": "healthy",
  "version": "dev",
  "uptime": "1h23m45s",
  "checks": { "redis": "ok", "postgres": "ok" }
}
```

### GET /health/live

Liveness probe — `200 OK` if process is alive.

### GET /metrics

Prometheus metrics endpoint.

| Metric | Type | Labels |
|--------|------|--------|
| `pulseflow_events_ingested_total` | Counter | `source`, `type` |
| `pulseflow_events_processed_total` | Counter | `source`, `type` |
| `pulseflow_events_dlq_total` | Counter | — |
| `pulseflow_events_dlq_locked_total` | Counter | — |
| `pulseflow_stream_claims_total` | Counter | — |
| `pulseflow_processing_duration_seconds` | Histogram | `phase` |

## Web Dashboard

The PulseFlow dashboard is a Flutter-based admin interface (Web / Desktop / Mobile) providing a visual control plane for operators.

### Screens

| Screen | Description |
|--------|-------------|
| **Live Stream Monitor** | Real-time graphs of ingestion RPS, processing RPS, DLQ count, failure rate, active consumers, and circuit breaker state via WebSocket. |
| **DLQ Operator** | List dead-lettered messages with filters, bulk retry, purge, and per-message detail view. |
| **Payload Inspector** | Search historical events by type/source/subject, inspect JSON payloads and headers, and trigger time-travel replay of individual events. |
| **Circuit Breaker Panel** | View live breaker state and 1-click manual reset with reset history log. |

### Build & Run

```bash
cd dashboard_app
flutter pub get
flutter run -d chrome  # Web
flutter run -d macos   # Desktop
# Mobile: flutter run
```

Configure the dashboard to connect to the API:

```bash
flutter run -d chrome \
  --dart-define=PULSEFLOW_API_URL=http://localhost:8080 \
  --dart-define=PULSEFLOW_API_KEY=pk_live_xxx
```

## Security

### Webhook Signing

All webhook payloads are signed with HMAC-SHA256:

```
signature = HMAC-SHA256(secret, timestamp + "." + hex(payload))
```

When dual-secret rotation is active (secondary secret configured), the signature header contains both keys:

```
X-PulseFlow-Signature: v1=<hash_primary>,v1=<hash_secondary>
```

Receivers should verify against **either** signature. This enables zero-downtime key rotation — see [Advanced Webhook Delivery](#advanced-webhook-delivery) for the rotation workflow.

Headers:

| Header | Description |
|--------|-------------|
| `Content-Type` | `application/json` |
| `X-PulseFlow-Timestamp` | Unix timestamp (anti-replay) |
| `X-PulseFlow-Signature` | HMAC-SHA256 hex digest |

Receivers must reject signatures older than a tolerance window (recommended: 300s) and verify using `hmac.Equal`.

### Input Validation

- All event data validated via `Event.Validate()` before publishing
- HTTP bodies capped by `server.max_body_bytes` (default 10MB)
- Rate limiting per IP (default 200 RPS, burst ×2)
- ULIDs prevent sequential ID guessing

### Webhook Configuration

```yaml
webhooks:
  enabled: true
  endpoints:
    - url: https://example.com/webhook
      secret: my-shared-secret
      timeout: 5s
      retries: 3
      retry_delay: 200ms
```

## Observability

PulseFlow sends traces to any OpenTelemetry-compatible backend (Jaeger, Tempo, Honeycomb, Datadog, etc.). Tracing is off by default.

```yaml
tracing:
  enabled: true
  service_name: "pulseflow"
  exporter: "otlphttp"
  endpoint: "localhost:4318"
  sample_rate: 1.0
```

**Supported exporters:**
- `otlphttp` — OTLP HTTP exporter (default, port 4318)
- `otlpgrpc` — OTLP gRPC exporter (port 4317)

Each HTTP request gets a trace span. Trace context (trace ID, span ID) flows through Redis Streams, so you can follow an event from ingestion through processing to webhook delivery.

### Distributed Tracing Flow

```
HTTP Request → Tracing Middleware (start span) → IngestUseCase (inject trace_id/span_id)
  → Redis Stream (propagate metadata) → ProcessUseCase (start child span)
  → Webhook Sender (propagate traceparent to downstream)
```

## Advanced Webhook Delivery

This section covers how webhook delivery works and how to manage destination endpoints.

### Rotating Webhook Signing Keys

Webhook destinations support zero-downtime secret rotation. When rotating a signing secret:

1. Set `secondary_secret` to the new key and `rotation_expires_at` to the rotation deadline (e.g. 24h).
2. During the rotation window, outgoing payloads are signed with **both** the primary and secondary secrets.
3. Receivers verify against either key.
4. Once the rotation window expires, promote the secondary to primary and clear the secondary field.

**Signature header format:**
```
X-PulseFlow-Signature: v1=<hash_primary>,v1=<hash_secondary>
```

When only the primary secret is configured (no rotation in progress), a single `v1=<hash_primary>` is emitted.

### HTTP 410 Gone Auto-Disabling

If a webhook destination responds with HTTP `410 Gone`, the destination is immediately marked as `disabled` in PostgreSQL. The consumer skips disabled destinations for all subsequent deliveries, preventing repeated failed deliveries to decommissioned endpoints.

### Filtering Events with CEL

Each destination can have a CEL expression that decides which events it receives. The expression runs before delivery. If it returns false, the event is skipped without wasting a retry attempt.

Available variables in CEL expressions:
- `event.id` — event ULID
- `event.type` — event type (e.g. `"order.created"`)
- `event.source` — event source
- `event.subject` — event subject
- `event.data` — parsed JSON payload (e.g. `event.data.amount`)
- `event.metadata` — metadata map (e.g. `event.metadata.trace_id`)
- `event.timestamp` — ISO-8601 timestamp string
- `event.version` — schema version

**Examples:**
```
event.type == 'order.created' && event.data.amount > 100
event.source == 'billing-service' && event.data.status == 'succeeded'
event.metadata['priority'] == 'high' || event.data.amount > 1000
```

### Limiting Concurrent Requests Per Destination

Each destination has a `concurrency_limit` (default: 5). If more than this many webhook deliveries are in flight at the same time for the same destination, new deliveries are deferred until a slot frees up. This prevents overloading downstream services without wasting retry attempts.

The concurrency limiter is implemented as a Redis-backed distributed semaphore using a Lua script for atomic increment/check/decrement operations, with a 60-second TTL to automatically clean up stale locks from crashed workers.

## Deployment

### Docker

```dockerfile
FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /pulseflow ./cmd/server
FROM alpine:3.20
COPY --from=builder /pulseflow /pulseflow
COPY config.yaml /app/config.yaml
EXPOSE 8080
ENTRYPOINT ["/pulseflow"]
```

### Docker Compose

```bash
cd deploy
docker compose up --build
```

Full stack: pulseflow, Redis, PostgreSQL, optional Tempo + OTEL Collector for tracing.

### Kubernetes

```yaml
replicaCount: 3
image: ghcr.io/ayoubzulfiqar/pulseflow:latest
livenessProbe:
  httpGet: { path: /health/live, port: 8080 }
readinessProbe:
  httpGet: { path: /health, port: 8080 }
```

## Failure Modes & Recovery

| Scenario | Detection | Recovery | Impact |
|---|---|---|---|
| Redis unavailable | Ping fails | Circuit breaker trips; ingest continues if Postgres OK | Events stored but not processed |
| Postgres unavailable | Ping fails | Circuit breaker isolates; stream publish continues | No durable replay source |
| Consumer crash | No ACK | XCLAIM reclaims messages after 60s | ~60s processing delay |
| Webhook endpoint down | HTTP 5xx | Exponential backoff (200ms→400ms→800ms→...) | Non-blocking; ingestion unaffected |
| Processing failure | Processor error | Requeue to DLQ; after 5 retries, permanently locked | Isolated per-event |

### Graceful Shutdown

On `SIGINT`/`SIGTERM`:
1. HTTP server stops accepting new connections
2. In-flight requests complete within `server.shutdown_timeout` (default 20s)
3. Consumer workers finish in-flight messages
4. Connection pools close
5. Process exits 0

## Development

### Prerequisites

- Go 1.25+
- Redis 7+
- PostgreSQL 16+

### Build & Verify

```bash
go mod tidy
go build ./...
go vet ./...
go test ./...
```

### Project Structure

```
pulseflow/
├── cmd/server/main.go          # Entry point + graceful shutdown
├── config.yaml                  # Default configuration
├── deploy/
│   ├── Dockerfile              # Multi-stage build
│   └── docker-compose.yml      # Full stack with tracing
├── dashboard_app/               # Flutter control-plane dashboard (Web/Desktop/Mobile)
│   ├── pubspec.yaml
│   ├── lib/
│   │   ├── main.dart
│   │   ├── models/models.dart   # Data models (Event, DLQMessage, MetricsEvent, etc.)
│   │   ├── services/
│   │   │   ├── api_client.dart  # REST API client
│   │   │   └── ws_client.dart   # WebSocket metrics client
│   │   └── screens/
│   │       ├── live_stream_monitor.dart  # Real-time graphs + metrics
│   │       ├── dlq_operator.dart        # DLQ list/retry/purge
│   │       ├── payload_inspector.dart   # Event search & replay
│   │       └── circuit_breaker_panel.dart # Breaker state & reset
│   └── web/index.html
├── internal/
│   ├── adapter/
│   │   ├── api/                # HTTP layer (Fiber)
│   │   │   ├── handler.go      # Route handlers (ingest, query, admin, ws)
│   │   │   ├── middleware.go   # RequestID, Logger, Recover, RateLimiter
│   │   │   ├── auth.go         # API key auth + per-tenant rate limiting
│   │   │   ├── admin.go        # DLQ, replay, circuit breaker endpoints
│   │   │   ├── websocket.go    # WebSocket broadcaster + metrics streaming
│   │   │   └── middleware/
│   │   │       └── tracing.go  # OpenTelemetry distributed tracing middleware
│   │   ├── postgres/           # Persistence (pgx v5, embedded migrations)
│   │   │   ├── event.go        # EventRepository + partition management
│   │   │   ├── dlq.go          # DLQRepository methods
│   │   │   ├── tenant.go       # TenantRepository (multi-tenant)
│   │   │   ├── destination.go  # DestinationRepository (webhook targets)
│   │   │   └── migrations/
│   │   ├── redis/              # Streaming (XADD, XREADGROUP, XCLAIM, DLQ)
│   │   │   ├── stream.go       # Stream adapter + DLQ management
│   │   │   ├── concurrency.go  # Redis-backed concurrency limiter (Lua script)
│   │   │   ├── circuitbreaker.go  # sony/gobreaker wrapper
│   │   │   └── circuitbreaker_state.go  # State adapter for admin queries
│   │   ├── tracing/            # OpenTelemetry provider + propagator
│   │   │   ├── tracing.go      # InitTracer + OTLP HTTP/gRPC exporter
│   │   │   └── propagator.go   # W3C TraceContext HTTP/Redis propagation
│   │   ├── filter/             # CEL payload filtering
│   │   │   └── cel.go          # RuleEngine with compiled-program cache
│   │   └── webhook/            # Signed webhook delivery
│   │       └── sender.go       # Dual-secret HMAC-SHA256, 410 auto-disable, backoff
│   ├── config/                 # Viper-based loader
│   ├── entity/                 # Domain (zero deps)
│   │   ├── event.go            # Event entity, validation
│   │   ├── dlq.go              # DLQMessage entity, DLQFilter
│   │   ├── tenant.go           # Tenant, APIKey entities
│   │   ├── destination.go      # Destination entity (dual-secret, CEL, concurrency)
│   │   ├── destination_repository.go  # Ports for destinations, webhooks, CEL, concurrency
│   │   ├── tracing.go          # Propagator interface, Tracer alias, trace keys
│   │   ├── errors.go           # Domain error sentinels
│   │   └── repository.go       # EventRepository, EventStream ports
│   └── usecase/                # Application layer
│       ├── ingest.go
│       ├── process.go          # Consumer workers + webhook delivery + tracing
│       ├── query.go
│       ├── dlq.go              # DLQ management use case
│       ├── replay.go           # Time-travel replay engine
│       ├── webhook.go          # Webhook deliverer interface
│       ├── metrics.go          # Prometheus metric wrappers
│       └── errors.go           # Usecase error sentinels
├── go.mod
└── README.md
```

### Smoke Test

```bash
go build -o bin/pulseflow ./cmd/server
./bin/pulseflow
curl http://localhost:8080/health
curl -X POST localhost:8080/v1/events \
  -H "Content-Type: application/json" \
  -d '{"source":"test","type":"ping","subject":"system","data":{"msg":"hello"}}'
curl "http://localhost:8080/v1/events?types=ping&limit=10"
curl http://localhost:8080/metrics
```

## License

Proprietary — developed by Ayoub Zulfiqar.
