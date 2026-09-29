package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/config"
	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/ayoubzulfiqar/pulseflow/internal/usecase"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// IngestRequest is the JSON body for POST /v1/events.
type IngestRequest struct {
	Source    string            `json:"source"`
	Type      string            `json:"type"`
	Subject   string            `json:"subject"`
	Data      json.RawMessage   `json:"data"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Timestamp *time.Time        `json:"timestamp,omitempty"`
	Version   string            `json:"version,omitempty"`
}

// toEvent converts the request into a domain Event entity.
func (req *IngestRequest) toEvent() *entity.Event {
	e := &entity.Event{
		Source:   req.Source,
		Type:     entity.EventType(req.Type),
		Subject:  req.Subject,
		Data:     req.Data,
		Metadata: req.Metadata,
		Version:  req.Version,
	}
	if req.Timestamp != nil {
		e.Timestamp = *req.Timestamp
	}
	return e
}

// ListResponse wraps a paginated list of events.
type ListResponse struct {
	Data   []*entity.Event `json:"data"`
	Count  int             `json:"count"`
	Offset int             `json:"offset"`
	Limit  int             `json:"limit"`
}

// HealthResponse is returned by GET /health.
type HealthResponse struct {
	Status   string            `json:"status"`
	Version  string            `json:"version"`
	Uptime   string            `json:"uptime"`
	Checks   map[string]string `json:"checks"`
}

// Server wraps the Fiber application with use-case dependencies.
type Server struct {
	app           *fiber.App
	cfg           *config.Config
	ingestUC      *usecase.IngestUseCase
	queryUC       *usecase.QueryUseCase
	logger        *slog.Logger
	metrics       *usecase.Metrics
	startTime     time.Time
	redisHealthy  func() bool
	dbHealthy     func() bool
}

// ServerOption configures the Server.
type ServerOption func(*Server)

// WithHealthChecks injects live health-check functions for Redis and Postgres.
func WithHealthChecks(redisFn, dbFn func() bool) ServerOption {
	return func(s *Server) {
		s.redisHealthy = redisFn
		s.dbHealthy = dbFn
	}
}

// NewServer creates and configures the Fiber HTTP server.
func NewServer(
	cfg *config.Config,
	ingestUC *usecase.IngestUseCase,
	queryUC *usecase.QueryUseCase,
	logger *slog.Logger,
	metrics *usecase.Metrics,
	opts ...ServerOption,
) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	app := fiber.New(fiber.Config{
		ServerHeader:    "PulseFlow",
		ReadTimeout:     cfg.Server.ReadTimeout,
		WriteTimeout:    cfg.Server.WriteTimeout,
		IdleTimeout:     cfg.Server.IdleTimeout,
		ReadBufferSize:  4 * 1024,
		WriteBufferSize: 4 * 1024,
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
			}
			logger.Error("request error", "error", err, "path", c.Path(), "method", c.Method(), "status", code)
			return c.Status(code).JSON(fiber.Map{
				"error": err.Error(),
			})
		},
	})

	s := &Server{
		app:        app,
		cfg:        cfg,
		ingestUC:   ingestUC,
		queryUC:    queryUC,
		logger:     logger,
		metrics:    metrics,
		startTime:  time.Now(),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.registerMiddleware()
	s.registerRoutes()

	return s
}

// App returns the underlying Fiber application (for Listen/Shutdown).
func (s *Server) App() *fiber.App {
	return s.app
}

func (s *Server) registerMiddleware() {
	s.app.Use(RequestID())
	s.app.Use(Logger(s.logger))
	s.app.Use(Recover(s.logger))
}

func (s *Server) registerRoutes() {
	v1 := s.app.Group("/v1")
	v1.Post("/events", s.Ingest)
	v1.Get("/events", s.Query)
	v1.Get("/events/:id", s.GetByID)

	s.app.Get("/health", s.Health)
	s.app.Get("/health/live", s.LiveProbe)

	if s.cfg.Metrics.Enabled {
		mh := promhttp.Handler()
		s.app.Get(s.cfg.Metrics.Path, adaptor.HTTPHandler(http.HandlerFunc(mh.ServeHTTP)))
	}
}

// Ingest handles POST /v1/events — validates and publishes an event.
func (s *Server) Ingest(c *fiber.Ctx) error {
	var req IngestRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("invalid request body: %v", err),
		})
	}
	if len(strings.TrimSpace(req.Source)) == 0 {
		req.Source = "api"
	}
	if len(strings.TrimSpace(req.Type)) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "type is a required field",
		})
	}
	if len(strings.TrimSpace(req.Subject)) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "subject is a required field",
		})
	}
	if len(req.Data) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "data is a required field",
		})
	}

	event := req.toEvent()
	rid := c.Locals("request_id")

	logger := s.logger
	if ridStr, ok := rid.(string); ok && ridStr != "" {
		logger = logger.With("request_id", ridStr)
	}

	result, err := s.ingestUC.Ingest(c.UserContext(), event)
	if err != nil {
		logger.Error("ingest failed", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.Status(fiber.StatusCreated).JSON(result)
}

// Query handles GET /v1/events — returns events matching the filter.
func (s *Server) Query(c *fiber.Ctx) error {
	filter, err := parseEventFilter(c)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	events, err := s.queryUC.Query(c.UserContext(), filter)
	if err != nil {
		s.logger.Error("query failed", "error", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(ListResponse{
		Data:   events,
		Count:  len(events),
		Offset: filter.Offset,
		Limit:  filter.MaxLimit,
	})
}

// GetByID handles GET /v1/events/:id — returns a single event.
func (s *Server) GetByID(c *fiber.Ctx) error {
	id := entity.EventID(c.Params("id"))
	if len(id) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "event id is required",
		})
	}

	event, err := s.queryUC.GetByID(c.UserContext(), id)
	if err != nil {
		if isNotFound(err) {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
				"error": fmt.Sprintf("event not found: %s", id),
			})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(event)
}

// Health handles GET /health — returns service health status.
func (s *Server) Health(c *fiber.Ctx) error {
	checks := map[string]string{}

	if s.redisHealthy != nil {
		checks["redis"] = healthStatus(s.redisHealthy())
	} else {
		checks["redis"] = "unknown"
	}
	if s.dbHealthy != nil {
		checks["postgres"] = healthStatus(s.dbHealthy())
	} else {
		checks["postgres"] = "unknown"
	}

	status := fiber.StatusOK
	for _, v := range checks {
		if v == "unhealthy" {
			status = fiber.StatusServiceUnavailable
			break
		}
	}

	return c.Status(status).JSON(HealthResponse{
		Status:  aggregateStatus(checks),
		Version: s.cfg.App.Version,
		Uptime:  time.Since(s.startTime).String(),
		Checks:  checks,
	})
}

// LiveProbe handles GET /health/live — returns 200 if the process is alive.
func (s *Server) LiveProbe(c *fiber.Ctx) error {
	return c.Status(fiber.StatusOK).JSON(fiber.Map{
		"status": "alive",
		"uptime": time.Since(s.startTime).String(),
	})
}

// --- helpers ---

func healthStatus(ok bool) string {
	if ok {
		return "ok"
	}
	return "unhealthy"
}

func aggregateStatus(checks map[string]string) string {
	for _, v := range checks {
		if v == "unhealthy" {
			return "degraded"
		}
	}
	return "healthy"
}

func isNotFound(err error) bool {
	return strings.Contains(err.Error(), "not found")
}

func parseEventFilter(c *fiber.Ctx) (entity.EventFilter, error) {
	filter := entity.EventFilter{}

	types := c.Query("types")
	if types != "" {
		parts := strings.Split(types, ",")
		filter.Types = make([]entity.EventType, 0, len(parts))
		for _, p := range parts {
			filter.Types = append(filter.Types, entity.EventType(strings.TrimSpace(p)))
		}
	}

	sources := c.Query("sources")
	if sources != "" {
		filter.Sources = splitCSV(sources)
	}

	subjects := c.Query("subjects")
	if subjects != "" {
		filter.Subjects = splitCSV(subjects)
	}

	if fromStr := c.Query("from"); fromStr != "" {
		t, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			return filter, fmt.Errorf("invalid 'from' timestamp: %w", err)
		}
		filter.From = t
	}

	if toStr := c.Query("to"); toStr != "" {
		t, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			return filter, fmt.Errorf("invalid 'to' timestamp: %w", err)
		}
		filter.To = t
	}

	limit, err := strconv.Atoi(c.Query("limit", "100"))
	if err != nil || limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	filter.MaxLimit = limit

	offset, err := strconv.Atoi(c.Query("offset", "0"))
	if err != nil || offset < 0 {
		offset = 0
	}
	filter.Offset = offset

	return filter, nil
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
