package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// buildVersion is set via linker flags at build time.
var buildVersion = "dev"

const envPrefix = "PULSEFLOW"

// Config holds the entire application configuration.
type Config struct {
	App        AppConfig        `mapstructure:"app"`
	Server     ServerConfig     `mapstructure:"server"`
	Redis      RedisConfig      `mapstructure:"redis"`
	Postgres   PostgresConfig   `mapstructure:"postgres"`
	Events     EventsConfig     `mapstructure:"events"`
	Webhooks   WebhooksConfig   `mapstructure:"webhooks"`
	RateLimit  RateLimitConfig  `mapstructure:"rate_limit"`
	CircuitBreaker CircuitBreakerConfig `mapstructure:"circuit_breaker"`
	Logging    LoggingConfig    `mapstructure:"logging"`
	Metrics    MetricsConfig    `mapstructure:"metrics"`
	Tracing    TracingConfig    `mapstructure:"tracing"`
	Transformation TransformationConfig `mapstructure:"transformation"`
	Compliance   ComplianceConfig   `mapstructure:"compliance"`
	Embed       EmbedConfig      `mapstructure:"embed"`
	Admin      AdminConfig      `mapstructure:"admin"`
}

type AppConfig struct {
	Name    string `mapstructure:"name"`
	Version string `mapstructure:"version"`
	Env     string `mapstructure:"env"`
}

type ServerConfig struct {
	Host           string        `mapstructure:"host"`
	Port           int           `mapstructure:"port"`
	ReadTimeout    time.Duration `mapstructure:"read_timeout"`
	WriteTimeout   time.Duration `mapstructure:"write_timeout"`
	IdleTimeout    time.Duration `mapstructure:"idle_timeout"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
	MaxBodyBytes   int64         `mapstructure:"max_body_bytes"`
}

type RedisConfig struct {
	Addr         string `mapstructure:"addr"`
	Password     string `mapstructure:"password"`
	DB           int    `mapstructure:"db"`
	PoolSize     int    `mapstructure:"pool_size"`
	MinIdleConns int    `mapstructure:"min_idle_conns"`
	Required     bool   `mapstructure:"required"`
}

type PostgresConfig struct {
	DSN             string        `mapstructure:"dsn"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
	Required        bool          `mapstructure:"required"`
}

// EventsConfig controls the Redis Streams pipeline.
type EventsConfig struct {
	StreamName     string        `mapstructure:"stream_name"`
	GroupName      string        `mapstructure:"group_name"`
	DLQStreamName  string        `mapstructure:"dlq_stream_name"`
	ConsumerName   string        `mapstructure:"consumer_name"`
	BatchSize      int           `mapstructure:"batch_size"`
	MaxPending     int           `mapstructure:"max_pending"`
	ClaimScanInterval time.Duration `mapstructure:"claim_scan_interval"`
	ClaimMinIdle   time.Duration `mapstructure:"claim_min_idle"`
	ConsumerConcurrency int    `mapstructure:"consumer_concurrency"`
}

type WebhookConfig struct {
	URL      string        `mapstructure:"url"`
	Secret   string        `mapstructure:"secret"`
	Timeout  time.Duration `mapstructure:"timeout"`
	Retries  int           `mapstructure:"retries"`
	RetryDelay time.Duration `mapstructure:"retry_delay"`
}

type WebhooksConfig struct {
	Enabled    bool           `mapstructure:"enabled"`
	Timeout    time.Duration  `mapstructure:"timeout"`
	MaxRetries int            `mapstructure:"max_retries"`
	RetryDelay time.Duration  `mapstructure:"retry_delay"`
	Endpoints  []WebhookConfig `mapstructure:"endpoints"`
}

type RateLimitConfig struct {
	Enabled      bool    `mapstructure:"enabled"`
	DefaultRPS   float64 `mapstructure:"default_rps"`
	BurstMultiplier int  `mapstructure:"burst_multiplier"`
	Strategy     string  `mapstructure:"strategy"`
}

type CircuitBreakerConfig struct {
	Redis    BreakerConfig `mapstructure:"redis"`
	Postgres BreakerConfig `mapstructure:"postgres"`
}

type BreakerConfig struct {
	MaxFailures      int           `mapstructure:"max_failures"`
	ResetTimeout     time.Duration `mapstructure:"reset_timeout"`
	HalfOpenMaxCalls int           `mapstructure:"half_open_max_calls"`
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type MetricsConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Path    string `mapstructure:"path"`
}

type TracingConfig struct {
	Enabled      bool    `mapstructure:"enabled"`
	ServiceName  string  `mapstructure:"service_name"`
	Exporter     string  `mapstructure:"exporter"` // "otlphttp" | "otlpgrpc" | "stdout"
	Endpoint     string  `mapstructure:"endpoint"`
	SampleRate   float64 `mapstructure:"sample_rate"`
}

type TransformationConfig struct {
	Enabled  bool              `mapstructure:"enabled"`
	Timeout  time.Duration     `mapstructure:"timeout"`
	DefaultScript string       `mapstructure:"default_script"`
}

type ComplianceConfig struct {
	PIIEnabled     bool           `mapstructure:"pii_enabled"`
	AuditEnabled   bool           `mapstructure:"audit_enabled"`
	AuditSecret    string         `mapstructure:"audit_secret"`
	RedactDLQ      bool           `mapstructure:"redact_dlq"`
	RedactLogs     bool           `mapstructure:"redact_logs"`
}

type EmbedConfig struct {
	Enabled     bool          `mapstructure:"enabled"`
	TokenSecret string        `mapstructure:"token_secret"`
	TokenTTL    time.Duration `mapstructure:"token_ttl"`
}

// AdminConfig controls the admin control plane.
type AdminConfig struct {
	AuthRequired bool `mapstructure:"auth_required"`
}

// Add Admin to Config.
// (inserted below)

// LoadConfig reads configuration from file and environment variables.
// When configPath is empty, it searches: ./config.yaml, $PWD/config.yaml,
// and the executable directory.
func LoadConfig(configPath string) (*Config, error) {
	v := viper.New()

	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	v.SetConfigType("yaml")

	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("config")
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/pulseflow")
	}

	if err := v.ReadInConfig(); err != nil {
		var notFoundErr viper.ConfigFileNotFoundError
		if !errors.As(err, &notFoundErr) {
			return nil, fmt.Errorf("config: read file: %w", err)
		}
	}

	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", 30*time.Second)
	v.SetDefault("server.write_timeout", 120*time.Second)
	v.SetDefault("server.idle_timeout", 120*time.Second)
	v.SetDefault("server.shutdown_timeout", 20*time.Second)
	v.SetDefault("server.max_body_bytes", 10*1024*1024)
	v.SetDefault("redis.addr", "localhost:6379")
	v.SetDefault("redis.pool_size", 25)
	v.SetDefault("redis.min_idle_conns", 5)
	v.SetDefault("postgres.dsn", "postgres://postgres:postgres@localhost:5432/pulseflow?sslmode=disable")
	v.SetDefault("postgres.max_open_conns", 25)
	v.SetDefault("postgres.max_idle_conns", 5)
	v.SetDefault("postgres.conn_max_lifetime", 30*time.Minute)
	v.SetDefault("events.stream_name", "pulseflow:events")
	v.SetDefault("events.group_name", "pulseflow:workers")
	v.SetDefault("events.dlq_stream_name", "pulseflow:dlq")
	v.SetDefault("events.batch_size", 100)
	v.SetDefault("events.max_pending", 500)
	v.SetDefault("events.claim_scan_interval", 30*time.Second)
	v.SetDefault("events.claim_min_idle", 60*time.Second)
	v.SetDefault("events.consumer_concurrency", 4)
	v.SetDefault("rate_limit.enabled", true)
	v.SetDefault("rate_limit.default_rps", 200.0)
	v.SetDefault("rate_limit.burst_multiplier", 2)
	v.SetDefault("circuit_breaker.redis.max_failures", 5)
	v.SetDefault("circuit_breaker.redis.reset_timeout", 60*time.Second)
	v.SetDefault("circuit_breaker.redis.half_open_max_calls", 3)
	v.SetDefault("circuit_breaker.postgres.max_failures", 5)
	v.SetDefault("circuit_breaker.postgres.reset_timeout", 60*time.Second)
	v.SetDefault("circuit_breaker.postgres.half_open_max_calls", 3)
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("metrics.enabled", true)
	v.SetDefault("metrics.path", "/metrics")
	v.SetDefault("tracing.enabled", false)
	v.SetDefault("tracing.service_name", "pulseflow")
	v.SetDefault("tracing.exporter", "otlphttp")
	v.SetDefault("tracing.endpoint", "localhost:4318")
	v.SetDefault("tracing.sample_rate", 1.0)
	v.SetDefault("transformation.enabled", false)
	v.SetDefault("transformation.timeout", 500*time.Millisecond)
	v.SetDefault("compliance.pii_enabled", false)
	v.SetDefault("compliance.audit_enabled", false)
	v.SetDefault("embed.enabled", false)
	v.SetDefault("embed.token_secret", "")
	v.SetDefault("embed.token_ttl", 24*time.Hour)
	v.SetDefault("admin.auth_required", false)
	v.SetDefault("webhooks.enabled", false)
	v.SetDefault("webhooks.timeout", 30*time.Second)
	v.SetDefault("webhooks.max_retries", 3)
	v.SetDefault("webhooks.retry_delay", 5*time.Second)

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("config: unmarshal: %w", err)
	}

	cfg.App.Version = buildVersion

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("config: validate: %w", err)
	}

	return &cfg, nil
}

func validate(cfg *Config) error {
	if cfg.Server.Port < 0 || cfg.Server.Port > 65535 {
		return errors.New("server.port must be between 0 and 65535")
	}
	if cfg.Events.BatchSize <= 0 {
		return errors.New("events.batch_size must be greater than 0")
	}
	if cfg.Events.ConsumerConcurrency < 1 {
		return errors.New("events.consumer_concurrency must be at least 1")
	}
	if cfg.RateLimit.DefaultRPS <= 0 && cfg.RateLimit.Enabled {
		return errors.New("rate_limit.default_rps must be greater than 0")
	}
	if len(cfg.Redis.Addr) == 0 {
		return errors.New("redis.addr must not be empty")
	}
	if len(cfg.Postgres.DSN) == 0 {
		return errors.New("postgres.dsn must not be empty")
	}
	if cfg.Postgres.MaxOpenConns > 0 && cfg.Postgres.MaxIdleConns > cfg.Postgres.MaxOpenConns {
		return errors.New("postgres.max_idle_conns cannot exceed postgres.max_open_conns")
	}
	return nil
}
