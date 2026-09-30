package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

// JSONSchemaValidator implements entity.SchemaValidator using the
// santhosh-tekuri/jsonschema library.
type JSONSchemaValidator struct {
	logger   *slog.Logger
	mu       sync.RWMutex
	schemas  map[string]*jsonschema.Schema // keyed by "tenant_id:event_pattern"
}

// NewJSONSchemaValidator creates a validator with the given logger.
func NewJSONSchemaValidator(logger *slog.Logger) *JSONSchemaValidator {
	if logger == nil {
		logger = slog.Default()
	}
	return &JSONSchemaValidator{
		logger:  logger,
		schemas: make(map[string]*jsonschema.Schema),
	}
}

// Validate checks the event's Data against matching registered schemas.
// If no schema matches, validation passes (no schema = no constraint).
func (v *JSONSchemaValidator) Validate(ctx context.Context, event *entity.Event) *entity.SchemaValidationResult {
	v.mu.RLock()
	defer v.mu.RUnlock()

	schema := v.matchSchema(event.Type, event.Source)
	if schema == nil {
		return &entity.SchemaValidationResult{Valid: true}
	}

	return v.validateAgainst(event.Data, schema)
}

// ValidateWithSchema validates the event data against a specific schema definition.
func (v *JSONSchemaValidator) ValidateWithSchema(ctx context.Context, event *entity.Event, def *entity.SchemaDefinition) *entity.SchemaValidationResult {
	schema, err := compileSchema(def.EventPattern, def.Schema)
	if err != nil {
		return &entity.SchemaValidationResult{
			Valid:  false,
			Errors: []string{fmt.Sprintf("compile schema: %v", err)},
		}
	}
	return v.validateAgainst(event.Data, schema)
}

// RegisterSchema compiles and stores a schema for future validation.
func (v *JSONSchemaValidator) RegisterSchema(ctx context.Context, schema *entity.SchemaDefinition) error {
	compiled, err := compileSchema(schema.EventPattern, schema.Schema)
	if err != nil {
		return fmt.Errorf("schema: compile failed for %s: %w", schema.EventPattern, err)
	}

	key := schema.EventPattern
	if schema.TenantID != "" {
		key = schema.TenantID + ":" + schema.EventPattern
	}
	v.mu.Lock()
	v.schemas[key] = compiled
	v.mu.Unlock()

	return nil
}

// GetSchema retrieves a registered schema by event type.
// Note: compiled schemas cannot be re-serialized, so only the pattern
// metadata is returned.
func (v *JSONSchemaValidator) GetSchema(ctx context.Context, eventType entity.EventType, source string) (*entity.SchemaDefinition, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if key := fmt.Sprintf("%s:%s", source, string(eventType)); v.schemas[key] != nil {
		return &entity.SchemaDefinition{EventPattern: string(eventType), TenantID: source}, nil
	}
	if v.schemas[source+":*"] != nil {
		return &entity.SchemaDefinition{EventPattern: "*", TenantID: source}, nil
	}

	return nil, fmt.Errorf("schema: no schema registered for %s:%s", source, string(eventType))
}

// matchSchema returns the first matching schema for the event type and source.
func (v *JSONSchemaValidator) matchSchema(eventType entity.EventType, source string) *jsonschema.Schema {
	// Try exact match: "source:type"
	key := fmt.Sprintf("%s:%s", source, string(eventType))
	if s := v.schemas[key]; s != nil {
		return s
	}
	// Try source wildcard: "source:*"
	return v.schemas[source + ":*"]
}

// compileSchema compiles a JSON Schema from raw bytes using the
// santhosh-tekuri/jsonschema/v5 library. We register the schema as a
// named resource, then compile by name.
func compileSchema(name string, schemaJSON json.RawMessage) (*jsonschema.Schema, error) {
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(name, bytes.NewReader(schemaJSON)); err != nil {
		return nil, fmt.Errorf("add resource: %w", err)
	}
	schema, err := compiler.Compile(name)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	return schema, nil
}

// validateAgainst validates JSON data against a compiled schema.
func (v *JSONSchemaValidator) validateAgainst(data json.RawMessage, schema *jsonschema.Schema) *entity.SchemaValidationResult {
	if len(data) == 0 {
		return &entity.SchemaValidationResult{Valid: true}
	}

	var v_data any
	if err := json.Unmarshal(data, &v_data); err != nil {
		return &entity.SchemaValidationResult{
			Valid:  false,
			Errors: []string{fmt.Sprintf("invalid JSON: %v", err)},
		}
	}

	err := schema.Validate(v_data)
	if err != nil {
		return &entity.SchemaValidationResult{
			Valid:  false,
			Errors: []string{err.Error()},
		}
	}

	return &entity.SchemaValidationResult{Valid: true}
}

// Ensure JSONSchemaValidator satisfies entity.SchemaValidator.
var _ entity.SchemaValidator = (*JSONSchemaValidator)(nil)
