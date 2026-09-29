package filter

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/checker/decls"
	"cel.dev/cel-go/ext"
)

// RuleEngine implements entity.CELFilterer using cel-go.
// It caches compiled programs per expression string to avoid recompilation.
type RuleEngine struct {
	mu       sync.RWMutex
	programs map[string]cel.Program
}

// NewRuleEngine creates a CEL-based rule engine.
func NewRuleEngine() *RuleEngine {
	return &RuleEngine{
		programs: make(map[string]cel.Program),
	}
}

// ShouldDeliver evaluates the given CEL expression against the event.
// Returns true (deliver) if the expression is empty.
// Returns false (skip) if the expression evaluates to false.
func (e *RuleEngine) ShouldDeliver(ctx context.Context, expr string, event *entity.Event) (bool, error) {
	if expr == "" {
		return true, nil
	}

	prog, err := e.getProgram(expr)
	if err != nil {
		return false, fmt.Errorf("filter: compile expression: %w", err)
	}

	// Build the evaluation input from the event entity.
	var eventData map[string]any
	if len(event.Data) > 0 {
		if err := json.Unmarshal(event.Data, &eventData); err != nil {
			return false, fmt.Errorf("filter: unmarshal event data: %w", err)
		}
	} else {
		eventData = map[string]any{}
	}

	input := map[string]any{
		"event": map[string]any{
			"id":        string(event.ID),
			"source":    event.Source,
			"type":      string(event.Type),
			"subject":   event.Subject,
			"data":      eventData,
			"metadata":  event.Metadata,
			"timestamp": event.Timestamp.Format("2006-01-02T15:04:05Z"),
			"version":   event.Version,
		},
	}

	out, _, err := prog.ContextEval(ctx, input)
	if err != nil {
		return false, fmt.Errorf("filter: evaluate expression: %w", err)
	}

	match, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("filter: expression did not return a boolean")
	}

	return match, nil
}

// getProgram returns a cached compiled CEL program, compiling it if necessary.
func (e *RuleEngine) getProgram(expr string) (cel.Program, error) {
	e.mu.RLock()
	if prog, ok := e.programs[expr]; ok {
		e.mu.RUnlock()
		return prog, nil
	}
	e.mu.RUnlock()

	e.mu.Lock()
	defer e.mu.Unlock()

	// Double-check after acquiring write lock.
	if prog, ok := e.programs[expr]; ok {
		return prog, nil
	}

	env, err := cel.NewEnv(
		celVariableDecl(),
		ext.Strings(),
		ext.Encoders(),
	)
	if err != nil {
		return nil, fmt.Errorf("filter: create cel env: %w", err)
	}

	ast, iss := env.Compile(expr)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("filter: compile: %w", iss.Err())
	}

	compiled, err := env.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("filter: program: %w", err)
	}

	e.programs[expr] = compiled
	return compiled, nil
}

// celVariableDecl returns the CEL environment option declaring the
// "event" variable as a map type so expressions can reference
// event.type, event.data, event.source, event.subject, event.metadata.
func celVariableDecl() cel.EnvOption {
	return cel.Declarations(
		decls.NewVar("event", decls.NewMapType(
			decls.String,
			decls.Dyn,
		)),
	)
}

// Ensure RuleEngine satisfies entity.CELFilterer.
var _ entity.CELFilterer = (*RuleEngine)(nil)
