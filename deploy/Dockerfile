# syntax=docker/dockerfile:1

# ── Builder ───────────────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Cache layer — copy go.mod + go.sum first so Docker caches dependencies.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build.
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X github.com/ayoubzulfiqar/pulseflow/internal/config.buildVersion=dev" \
    -o /out/pulseflow ./cmd/server

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM alpine:3.20

WORKDIR /app

RUN apk --no-cache add ca-certificates && \
    addgroup -S pulseflow && adduser -S -G pulseflow pulseflow

COPY --from=builder /out/pulseflow /app/pulseflow
COPY config.yaml /app/config.yaml

USER pulseflow

EXPOSE 8080

ENTRYPOINT ["/app/pulseflow"]
