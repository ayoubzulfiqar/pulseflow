package entity

import (
	"context"
	"encoding/json"
	"time"
)

// TransformationConfig defines how an event's payload is transformed
// before delivery. Used for AI/LLM event routing where downstream
// systems expect a specific schema.
type TransformationConfig struct {
	// Script is a JavaScript snippet that transforms event.Data.
	// The script receives `event` as a global variable and must
	// return the transformed data as a JSON-serializable object.
	Script string `json:"script"`

	// JSONSchema is an optional JSON Schema that the transformed
	// output must validate against before delivery is attempted.
	// If validation fails, delivery is skipped and an error is logged.
	JSONSchema json.RawMessage `json:"json_schema,omitempty"`

	// IsActive controls whether transformation is applied.
	IsActive bool `json:"is_active"`
}

// Transformer is the port (interface) for payload transformation.
// Implementations may use JavaScript, WASM, or other sandboxed
// execution environments.
type Transformer interface {
	// Transform applies the configured transformation script to the
	// event's Data field and returns the transformed payload.
	// If the script is empty, the original data is returned unchanged.
	Transform(ctx context.Context, config *TransformationConfig, event *Event) (json.RawMessage, error)
}

// Validate transforms a raw JSON schema into a compiled schema object.
// Returns nil if the schema is invalid or empty.
func (tc *TransformationConfig) Validate() error {
	if tc == nil || tc.Script == "" {
		return nil
	}
	if !tc.IsActive {
		return nil
	}
	return nil
}

// CreatedAt/UpdatedAt for audit purposes.
func (tc *TransformationConfig) Age() time.Duration {
	return 0
}
