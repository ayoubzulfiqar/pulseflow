package api

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/ayoubzulfiqar/pulseflow/internal/usecase"
	"github.com/gofiber/fiber/v2"
)

// AdminDeps holds dependencies for admin API endpoints.
type AdminDeps struct {
	DLQ        *usecase.DLQUseCase
	Replay     *usecase.EventReplayUseCase
	Metrics    *usecase.Metrics
	Stream     entity.EventStream
	CBResetFn  func(ctx context.Context) error // resets Redis circuit breaker
	TenantRepo entity.TenantRepository          // for auth lookups
}

// DLQListResponse is the response for GET /v1/admin/dlq.
type DLQListResponse struct {
	Data   []*entity.DLQMessage `json:"data"`
	Count  int                  `json:"count"`
	Offset int                  `json:"offset"`
	Limit  int                  `json:"limit"`
}

// DLQRetryResponse is the response for POST /v1/admin/dlq/retry.
type DLQRetryResponse struct {
	Requested int                       `json:"requested"`
	Requeued  int                       `json:"requeued"`
	Skipped   int                       `json:"skipped"`
	Failed    []usecase.DLQRetryFailure `json:"failed,omitempty"`
}

// ReplayResponse is the response for POST /v1/admin/replay.
type ReplayResponse struct {
	Replayed   int      `json:"replayed"`
	Successful int      `json:"successful"`
	Failed     int      `json:"failed"`
	Errors     []string `json:"errors,omitempty"`
}

// CircuitBreakerResponse describes the circuit breaker state.
type CircuitBreakerResponse struct {
	Name       string `json:"name"`
	State      string `json:"state"`
	Failing    int    `json:"failing"`
	TotalCalls int    `json:"total_calls"`
}

// RegisterAdminRoutes wires admin endpoints onto the v1 group.
func RegisterAdminRoutes(v1 fiber.Router, deps *AdminDeps) {
	admin := v1.Group("/admin")
	admin.Get("/dlq", dlqList(deps))
	admin.Post("/dlq/retry", dlqRetry(deps))
	admin.Delete("/dlq/purge", dlqPurge(deps))
	admin.Post("/replay", replay(deps))
	admin.Get("/circuit-breaker", cbState(deps))
	admin.Post("/circuit-breaker/reset", cbReset(deps))
}

// --- Handlers ---

// dlqList handles GET /v1/admin/dlq — list DLQ messages with filtering.
func dlqList(deps *AdminDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		filter, err := parseDLQFilter(c)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		msgs, err := deps.DLQ.ListDLQ(c.UserContext(), filter)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		return c.JSON(DLQListResponse{
			Data:   msgs,
			Count:  len(msgs),
			Offset: filter.Offset,
			Limit:  filter.MaxLimit,
		})
	}
}

// dlqRetry handles POST /v1/admin/dlq/retry — re-enqueue selected DLQ messages.
func dlqRetry(deps *AdminDeps) fiber.Handler {
	type req struct {
		IDs []string `json:"ids"`
	}
	return func(c *fiber.Ctx) error {
		var body req
		if err := c.BodyParser(&body); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("invalid request body: %v", err),
			})
		}
		if len(body.IDs) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "ids field is required and must contain at least one ID",
			})
		}

		result, err := deps.DLQ.RetryDLQ(c.UserContext(), body.IDs)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		return c.JSON(DLQRetryResponse{
			Requested: result.Requested,
			Requeued:  result.Requeued,
			Skipped:   result.Skipped,
			Failed:    result.Failed,
		})
	}
}

// dlqPurge handles DELETE /v1/admin/dlq/purge — purge the DLQ.
func dlqPurge(deps *AdminDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		archive := c.Query("archive", "true") == "true"

		deleted, err := deps.DLQ.PurgeDLQ(c.UserContext(), archive)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		return c.JSON(fiber.Map{
			"deleted": deleted,
		})
	}
}

// replay handles POST /v1/admin/replay — time-travel replay.
func replay(deps *AdminDeps) fiber.Handler {
	type req struct {
		From     string   `json:"from"`
		To       string   `json:"to"`
		Types    []string `json:"types"`
		Sources  []string `json:"sources"`
		Subjects []string `json:"subjects"`
		MaxEvents int     `json:"max_events"`
	}
	return func(c *fiber.Ctx) error {
		var body req
		if err := c.BodyParser(&body); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": fmt.Sprintf("invalid request body: %v", err),
			})
		}

		var from, to time.Time
		var parseErr error
		if body.From != "" {
			from, parseErr = time.Parse(time.RFC3339, body.From)
			if parseErr != nil {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": fmt.Sprintf("invalid 'from' timestamp: %v", parseErr),
				})
			}
		}
		if body.To != "" {
			to, parseErr = time.Parse(time.RFC3339, body.To)
			if parseErr != nil {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": fmt.Sprintf("invalid 'to' timestamp: %v", parseErr),
				})
			}
		}

		types := make([]entity.EventType, len(body.Types))
		for i, t := range body.Types {
			types[i] = entity.EventType(t)
		}

		replayReq := usecase.ReplayRequest{
			From:      from,
			To:        to,
			Types:     types,
			Sources:   body.Sources,
			Subjects:  body.Subjects,
			MaxEvents: body.MaxEvents,
		}

		result, err := deps.Replay.Replay(c.UserContext(), replayReq)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		return c.JSON(ReplayResponse{
			Replayed:   result.Replayed,
			Successful: result.Successful,
			Failed:     result.Failed,
			Errors:     result.Errors,
		})
	}
}

// cbState handles GET /v1/admin/circuit-breaker — return breaker state.
func cbState(deps *AdminDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if breaker, ok := deps.Stream.(entity.CircuitBreakerState); ok {
			return c.JSON(CircuitBreakerResponse{
				Name:       breaker.Name(),
				State:      breaker.State(),
				Failing:    breaker.Failing(),
				TotalCalls: breaker.TotalCalls(),
			})
		}
		return c.JSON(CircuitBreakerResponse{
			Name:  "redis-stream",
			State: "unknown",
		})
	}
}

// cbReset handles POST /v1/admin/circuit-breaker/reset — manual breaker reset.
func cbReset(deps *AdminDeps) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if deps.CBResetFn == nil {
			return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
				"error": "circuit breaker reset is not available",
			})
		}

		if err := deps.CBResetFn(c.UserContext()); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
			})
		}

		return c.JSON(fiber.Map{
			"status": "reset",
			"name":   "redis-stream",
		})
	}
}

// --- Helpers ---

// parseDLQFilter extracts a DLQFilter from HTTP query parameters.
func parseDLQFilter(c *fiber.Ctx) (entity.DLQFilter, error) {
	filter := entity.DLQFilter{}

	types := c.Query("types")
	if types != "" {
		parts := splitCSV(types)
		filter.Types = make([]entity.EventType, 0, len(parts))
		for _, p := range parts {
			filter.Types = append(filter.Types, entity.EventType(p))
		}
	}

	sources := c.Query("sources")
	if sources != "" {
		filter.Sources = splitCSV(sources)
	}

	status := c.Query("status")
	if status != "" {
		filter.Status = entity.DLQStatus(status)
	}

	if minRetry := c.QueryInt("min_retry"); minRetry > 0 {
		filter.MinRetry = minRetry
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
