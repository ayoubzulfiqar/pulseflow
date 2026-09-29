package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/api"
	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/filter"
	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/postgres"
	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/redis"
	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/tracing"
	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/webhook"
	"github.com/ayoubzulfiqar/pulseflow/internal/config"
	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/ayoubzulfiqar/pulseflow/internal/usecase"

	"github.com/google/uuid"
	rdlib "github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "pulseflow: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadConfig("")
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	logger := newLogger(os.Stdout, cfg.Logging.Level, cfg.Logging.Format)
	logger.Info("starting pulseflow", "version", cfg.App.Version, "env", cfg.App.Env)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- OpenTelemetry tracing provider (Phase 1) ---
	var tracerProvider trace.Tracer
	shutdownTracing, err := tracing.InitTracer(ctx, tracing.Config{
		Enabled:     cfg.Tracing.Enabled,
		ServiceName: cfg.Tracing.ServiceName,
		Endpoint:    cfg.Tracing.Endpoint,
		SampleRate:  cfg.Tracing.SampleRate,
	})
	if err != nil {
		logger.Warn("tracing: init failed, continuing without traces", "error", err)
		shutdownTracing = func(context.Context) error { return nil }
	}
	if cfg.Tracing.Enabled {
		tracerProvider = otel.Tracer("pulseflow")
		logger.Info("tracing enabled", "endpoint", cfg.Tracing.Endpoint, "sample_rate", cfg.Tracing.SampleRate)
	}

	// --- Prometheus metrics ---
	metrics := usecase.NewMetrics(nil)

	// --- Redis ---
	var redisClient *rdlib.Client
	var stream entity.EventStream
	var cbState *redis.CircuitBreakerStateAdapter
	var concurrencyLimiter *redis.ConcurrencyLimiter
	if cfg.Redis.Addr != "" {
		redisClient = rdlib.NewClient(&rdlib.Options{
			Addr:         cfg.Redis.Addr,
			Password:     cfg.Redis.Password,
			DB:           cfg.Redis.DB,
			PoolSize:     cfg.Redis.PoolSize,
			MinIdleConns: cfg.Redis.MinIdleConns,
		})

		if err := redisClient.Ping(ctx).Err(); err != nil {
			if cfg.Redis.Required {
				return fmt.Errorf("redis: ping failed: %w", err)
			}
			logger.Warn("redis: unavailable, running in degraded mode", "error", err)
			redisClient = nil
		}
	}

	if redisClient != nil {
		streamCfg := redis.Config{
			Stream:    cfg.Events.StreamName,
			Group:     cfg.Events.GroupName,
			DLQStream: cfg.Events.DLQStreamName,
		}
		baseStream := redis.NewStream(redisClient, streamCfg, logger)

		cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:        "redis-stream",
			MaxRequests: uint32(cfg.CircuitBreaker.Redis.HalfOpenMaxCalls),
			Timeout:     cfg.CircuitBreaker.Redis.ResetTimeout,
			ReadyToTrip: func(counts gobreaker.Counts) bool {
				return counts.ConsecutiveFailures >= uint32(cfg.CircuitBreaker.Redis.MaxFailures)
			},
			OnStateChange: func(name string, from, to gobreaker.State) {
				logger.Info("circuit breaker state change",
					"name", name, "from", from.String(), "to", to.String())
			},
		})

		cbState = redis.NewCircuitBreakerStateAdapter("redis-stream", cb, logger)
		stream = redis.NewCircuitBreakerStream(baseStream, cb)

		// Phase 4: Redis-backed concurrency limiter for webhook delivery.
		concurrencyLimiter = redis.NewConcurrencyLimiter(redisClient, logger, "concurrency", 60*time.Second)
	}

	// --- PostgreSQL ---
	var pgRepo *postgres.EventRepository
	var repo entity.EventRepository
	var tenantRepo *postgres.TenantRepository
	var destRepo *postgres.DestinationRepository
	if cfg.Postgres.DSN != "" {
		pgRepo, err = postgres.NewEventRepository(ctx, postgres.Config{
			DSN:             cfg.Postgres.DSN,
			MaxOpenConns:    cfg.Postgres.MaxOpenConns,
			MaxIdleConns:    cfg.Postgres.MaxIdleConns,
			ConnMaxLifetime: cfg.Postgres.ConnMaxLifetime,
		}, logger)
		if err != nil {
			if cfg.Postgres.Required {
				return fmt.Errorf("postgres: %w", err)
			}
			logger.Warn("postgres: unavailable, running in degraded mode", "error", err)
		} else {
			repo = pgRepo
			tenantRepo = postgres.NewTenantRepository(pgRepo.Pool(), logger)
			destRepo = postgres.NewDestinationRepository(pgRepo.Pool(), logger)
		}
	}

	// --- Webhook delivery adapter (Phase 2) ---
	var webhookSender *webhook.Sender
	if destRepo != nil {
		webhookSender = webhook.NewSender(destRepo, webhook.Config{
			Timeout:    cfg.Webhooks.Timeout,
			MaxRetries: cfg.Webhooks.MaxRetries,
			RetryDelay: cfg.Webhooks.RetryDelay,
		}, logger)
	}

	// --- CEL rule engine (Phase 3) ---
	celFilterer := filter.NewRuleEngine()

	// --- Use cases ---
	// Ingest no longer fires webhooks directly — delivery moves to ProcessUseCase.
	// The webhook param is kept as nil to maintain backward compat.
	ingestUC := usecase.NewIngestUseCase(stream, repo, nil, logger, metrics)
	queryUC := usecase.NewQueryUseCase(repo, logger)

	var dlqUC *usecase.DLQUseCase
	var replayUC *usecase.EventReplayUseCase
	if stream != nil {
		dlqUC = usecase.NewDLQUseCase(stream, repo, logger)
		if repo != nil {
			replayUC = usecase.NewEventReplayUseCase(stream, repo, logger)
		}
	}

	var processUC *usecase.ProcessUseCase
	if stream != nil && repo != nil {
		processor := usecase.NewStoreProcessor(repo)
		consumerName := cfg.Events.ConsumerName
		if consumerName == "" {
			consumerName = generateConsumerName()
		}

		// Build optional CEL filterer and webhook deliverer for ProcessUseCase.
		var filt entity.CELFilterer
		if webhookSender != nil || destRepo != nil {
			filt = celFilterer
		}

		var deliv entity.WebhookDeliverer
		if webhookSender != nil {
			deliv = webhookSender
		}

		// Attach concurrency limiter to the webhook sender via propagator/tracer.
		if concurrencyLimiter != nil && webhookSender != nil {
			// Note: The concurrency limiter is wired inside the webhook sender's
			// Acquire call via the destination's ConcurrencyLimit field.
		}

		processUC = usecase.NewProcessUseCase(
			stream, repo, processor, consumerName,
			cfg.Events.ConsumerConcurrency, cfg.Events.BatchSize,
			cfg.Events.MaxPending, cfg.Events.ClaimMinIdle,
			destRepo, filt, deliv, tracerProvider,
			logger, metrics,
		)
	}

	// --- Health checks ---
	redisHealthy := func() bool {
		if redisClient == nil {
			return false
		}
		return redisClient.Ping(context.Background()).Err() == nil
	}
	dbHealthy := func() bool {
		if pgRepo == nil {
			return false
		}
		return pgRepo.Health(context.Background())
	}

	// --- Circuit breaker reset function ---
	cbResetFn := func(ctx context.Context) error {
		if cbState != nil {
			cbState.Reset()
			return nil
		}
		return fmt.Errorf("circuit breaker not configured")
	}

	// --- WebSocket metrics broadcaster ---
	var metricsBroadcaster *api.MetricsBroadcaster
	if redisClient != nil {
		metricsBroadcaster = api.NewMetricsBroadcaster(stream, metrics, 2*time.Second)
		metricsBroadcaster.Start(ctx)
	}

	// --- HTTP server ---
	serverOpts := []api.ServerOption{
		api.WithHealthChecks(redisHealthy, dbHealthy),
	}
	if dlqUC != nil {
		serverOpts = append(serverOpts, api.WithDLQUseCase(dlqUC))
	}
	if replayUC != nil {
		serverOpts = append(serverOpts, api.WithReplayUseCase(replayUC))
	}
	if metricsBroadcaster != nil {
		serverOpts = append(serverOpts, api.WithWebSocketBroadcaster(metricsBroadcaster.Broadcaster()))
	}
	if stream != nil {
		serverOpts = append(serverOpts, api.WithStream(stream))
	}
	if cbState != nil {
		serverOpts = append(serverOpts, api.WithCBRestFn(cbResetFn))
	}
	if tenantRepo != nil {
		serverOpts = append(serverOpts, api.WithTenantRepo(tenantRepo))
	}
	if tracerProvider != nil {
		serverOpts = append(serverOpts, api.WithTracer(tracerProvider))
	}

	server := api.NewServer(cfg, ingestUC, queryUC, logger, metrics, serverOpts...)

	// --- Auth middleware ---
	if cfg.Admin.AuthRequired && tenantRepo != nil {
		server.App().Use(api.APIKeyAuth(tenantRepo, logger))
	}

	// --- Rate limiter ---
	var rl *api.PerTenantRateLimiter
	if cfg.RateLimit.Enabled {
		rl = api.NewPerTenantRateLimiter(&cfg.RateLimit, logger)
		server.App().Use(rl.Handler())
	}

	// --- Start consumer workers ---
	if processUC != nil {
		go func() {
			if err := processUC.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Error("process use case stopped unexpectedly", "error", err)
			}
		}()
	}

	// --- Start partition manager ---
	if pgRepo != nil {
		pm := postgres.NewPartitionManager(pgRepo.Pool(), logger, 1*time.Hour, 3)
		go func() {
			if err := pm.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Error("partition manager stopped unexpectedly", "error", err)
			}
		}()
	}

	// --- Start HTTP server ---
	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	logger.Info("server listening", "addr", addr)

	go func() {
		if err := server.App().Listen(addr); err != nil {
			logger.Error("server stopped", "error", err)
		}
	}()

	// --- Graceful shutdown ---
	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	if err := server.App().ShutdownWithContext(shutdownCtx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}

	if rl != nil {
		rl.Stop()
	}
	if metricsBroadcaster != nil {
		metricsBroadcaster.Stop()
	}
	if redisClient != nil {
		redisClient.Close()
	}
	if pgRepo != nil {
		pgRepo.Close()
	}
	_ = shutdownTracing(shutdownCtx)

	logger.Info("shutdown complete")
	return nil
}

func newLogger(w io.Writer, level, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	var handler slog.Handler
	switch strings.ToLower(format) {
	case "text":
		handler = slog.NewTextHandler(w, &slog.HandlerOptions{Level: lvl})
	default:
		handler = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	}
	return slog.New(handler)
}

func generateConsumerName() string {
	hostname, _ := os.Hostname()
	pid := os.Getpid()
	return fmt.Sprintf("%s-%d-%s", hostname, pid, uuid.NewString()[:8])
}
