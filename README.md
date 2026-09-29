# PulseFlow — Event-Driven Pipeline

Production-grade event ingestion, processing, and delivery platform. Built for high-throughput, fault-tolerant, observability-first event pipelines.

## Overview

PulseFlow is an event-driven pipeline that ingests domain events via HTTP, persists them durably, publishes them to a Redis Streams consumer-group topology for asynchronous processing, and delivers signed webhook notifications to downstream systems. Designed for horizontal scaling, zero-downtime deploys, and graceful degradation under partial infrastructure failure.

**Key differentiators:**
- Dual write path (durability + streaming) — events are persisted to PostgreSQL *and* published to Redis Streams before acknowledgement
- Consumer group with XCLAIM failover — dead workers' messages are automatically reclaimed
- Dead-letter queue with retry isolation — failed messages are quarantined after configurable retries (default: 5)
- HMAC-SHA256 webhook signing with timestamp anti-replay — signed payloads with configurable tolerance window
- Circuit breaker on the streaming layer — prevents cascade failures when Redis is unavailable
- Structured observability — Prometheus metrics, JSON structured logs

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
| **Domain** | `internal/entity` | Event entity, EventRepository/EventStream ports, validation rules |
| **Infrastructure** | `internal/adapter/{redis,postgres}` | Concrete Redis Streams, PostgreSQL, circuit breaker implementations |
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
}
```

### IngestUseCase

Validates, enriches, persists, and publishes incoming events. Idempotent on caller-supplied ULIDs.

### ProcessUseCase

Runs consumer workers that read from Redis Streams via consumer groups. Implements ack/nack, retry isolation with DLQ (max 5 retries, then permanently locked), XCLAIM failover for stale messages, and graceful shutdown.

### QueryUseCase

Provides paginated, filtered read access to the event store. Supports time-range, type, source, subject filtering, and pagination (limit max 1000, offset).

### Webhook Delivery

Asynchronous, fire-and-forget webhook delivery with HMAC-SHA256 signing, timestamp anti-replay, and exponential backoff retries (cenkalti/backoff/v5).

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
| | `tracing.sample_rate` | `1.0` | Trace sampling ratio |

### Environment Variables

All config keys support env overrides with `PULSEFLOW_` prefix:

```bash
PULSEFLOW_SERVER_PORT=9090
PULSEFLOW_REDIS_ADDR=redis:6379
PULSEFLOW_POSTGRES_DSN="postgres://user:pass@db:5432/pulseflow?sslmode=disable"
PULSEFLOW_LOGGING_LEVEL=debug
PULSEFLOW_WEBHOOKS_ENABLED=true
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

## Security

### Webhook Signing

All webhook payloads are signed with HMAC-SHA256:

```
signature = HMAC-SHA256(secret, timestamp + "." + hex(payload))
```

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

- Go 1.24+
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
├── internal/
│   ├── adapter/
│   │   ├── api/                # HTTP layer
│   │   ├── postgres/           # Persistence (pgx v5, embedded migrations)
│   │   └── redis/              # Streaming (XADD, XREADGROUP, XCLAIM, DLQ)
│   ├── config/                 # Viper-based loader
│   ├── entity/                 # Domain (zero deps)
│   └── usecase/                # Application layer
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
