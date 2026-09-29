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

	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/api"
	"github.com/ayoubzulfiqar/pulseflow/internal/adapter/postgres"
	redis "github.com/ayoubzulfiqar/pulseflow/internal/adapter/redis"
	"github.com/ayoubzulfiqar/pulseflow/internal/config"
	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/ayoubzulfiqar/pulseflow/internal/usecase"

	"github.com/google/uuid"
	rdlib "github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
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

	// --- Prometheus metrics ---
	metrics := usecase.NewMetrics(nil) // nil → default registry

	// --- Webhook sender (optional) ---
	var webhook *usecase.WebhookSender
	if cfg.Webhooks.Enabled && len(cfg.Webhooks.Endpoints) > 0 {
		endpoints := make([]usecase.WebhookConfig, 0, len(cfg.Webhooks.Endpoints))
		for _, ep := range cfg.Webhooks.Endpoints {
			endpoints = append(endpoints, usecase.WebhookConfig{
				URL:        ep.URL,
				Secret:     ep.Secret,
				Timeout:    ep.Timeout,
				Retries:    ep.Retries,
				RetryDelay: ep.RetryDelay,
			})
		}
		webhook = usecase.NewWebhookSender(endpoints, logger)
	}

	// --- Redis ---
	var redisClient *rdlib.Client
	var stream entity.EventStream
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
		stream = redis.NewCircuitBreakerStream(baseStream, cb)
	}

	// --- PostgreSQL ---
	var pgRepo *postgres.EventRepository
	var repo entity.EventRepository
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
		}
	}

	// --- Use cases ---
	ingestUC := usecase.NewIngestUseCase(stream, repo, webhook, logger, metrics)
	queryUC := usecase.NewQueryUseCase(repo, logger)

	var processUC *usecase.ProcessUseCase
	if stream != nil && repo != nil {
		processor := usecase.NewStoreProcessor(repo)
		consumerName := cfg.Events.ConsumerName
		if consumerName == "" {
			consumerName = generateConsumerName()
		}
		processUC = usecase.NewProcessUseCase(
			stream, repo, processor, consumerName,
			cfg.Events.ConsumerConcurrency, cfg.Events.BatchSize,
			cfg.Events.MaxPending, cfg.Events.ClaimMinIdle,
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

	// --- HTTP server ---
	server := api.NewServer(cfg, ingestUC, queryUC, logger, metrics,
		api.WithHealthChecks(redisHealthy, dbHealthy))

	// Rate limiter — registered after core middleware (RequestID, Logger, Recover)
	var rl *api.RateLimiter
	if cfg.RateLimit.Enabled {
		rl = api.NewRateLimiter(&cfg.RateLimit)
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
	if redisClient != nil {
		redisClient.Close()
	}
	if pgRepo != nil {
		pgRepo.Close()
	}

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
