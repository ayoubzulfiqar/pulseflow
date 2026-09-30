package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/robertkrimen/otto"
)

// JSTransformer implements entity.Transformer using a pure-Go JavaScript
// interpreter (otto). Scripts run in a sandboxed VM with a timeout.
type JSTransformer struct {
	timeout time.Duration
}

// NewJSTransformer creates a JS transformation adapter with the given
// execution timeout (e.g. 500ms).
func NewJSTransformer(timeout time.Duration) *JSTransformer {
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	return &JSTransformer{timeout: timeout}
}

// Transform executes the JS transformation script against the event's
// Data field. The script receives `event` as a global variable and
// must return the transformed data object.
//
// If the script is empty, the original data is returned unchanged.
func (t *JSTransformer) Transform(ctx context.Context, config *entity.TransformationConfig, event *entity.Event) (json.RawMessage, error) {
	if config == nil || config.Script == "" || !config.IsActive {
		return event.Data, nil
	}

	// Build the input data as a Go map for JS consumption.
	var data map[string]any
	if len(event.Data) > 0 {
		if err := json.Unmarshal(event.Data, &data); err != nil {
			return nil, fmt.Errorf("transform: unmarshal event data: %w", err)
		}
	} else {
		data = map[string]any{}
	}

	// Construct the JS execution context.
	vm := otto.New()

	// Set a deadline for script execution to prevent infinite loops.
	vm.Interrupt = make(chan func(), 1)
	timer := time.AfterFunc(t.timeout, func() {
		vm.Interrupt <- func() {
			panic("transform: script execution timed out")
		}
	})
	defer timer.Stop()

	// Expose the event as a global variable.
	_ = vm.Set("event", map[string]any{
		"id":       string(event.ID),
		"type":     string(event.Type),
		"source":   event.Source,
		"subject":  event.Subject,
		"data":     data,
		"metadata": event.Metadata,
		"version":  event.Version,
	})

	// Expose JSON helpers.
	_ = vm.Set("JSON", map[string]any{
		"parse":   jsonParse,
		"stringify": jsonStringify,
	})

	// Execute the transformation script.
	script := fmt.Sprintf(`
(function() {
  var result = (function(event) {
    %s
  })(event);
  if (result === undefined) {
    return event.data;
  }
  return result;
})();
`, config.Script)

	result, err := vm.Run(script)
	if err != nil {
		return nil, fmt.Errorf("transform: execute script: %w", err)
	}

	// Convert result to Go value, then marshal to JSON.
	resultVal, err := result.Export()
	if err != nil {
		return nil, fmt.Errorf("transform: export result: %w", err)
	}

	output, err := json.Marshal(resultVal)
	if err != nil {
		return nil, fmt.Errorf("transform: marshal result: %w", err)
	}

	// Validate against JSON schema if provided.
	if config.JSONSchema != nil && len(config.JSONSchema) > 0 {
		if err := validateJSONSchema(config.JSONSchema, output); err != nil {
			return nil, fmt.Errorf("transform: schema validation failed: %w", err)
		}
	}

	return output, nil
}

// jsonParse parses a JSON string into a Go object for use in JS.
func jsonParse(call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) == 0 {
		panic("JSON.parse requires an argument")
	}
	str := call.Argument(0).String()
	var result any
	if err := json.Unmarshal([]byte(str), &result); err != nil {
		panic(fmt.Sprintf("JSON.parse: %v", err))
	}
	val, _ := otto.ToValue(result)
	return val
}

// jsonStringify converts a JS object to a JSON string.
func jsonStringify(call otto.FunctionCall) otto.Value {
	if len(call.ArgumentList) == 0 {
		val, _ := otto.ToValue("undefined")
		return val
	}
	result, err := call.Argument(0).Export()
	if err != nil {
		panic(fmt.Sprintf("JSON.stringify: %v", err))
	}
	str, err := json.Marshal(result)
	if err != nil {
		panic(fmt.Sprintf("JSON.stringify: %v", err))
	}
	val, _ := otto.ToValue(string(str))
	return val
}

// jsonSchemaRegex is used for basic schema validation (structure-level checks).
var jsonSchemaRegex = regexp.MustCompile(`^\s*`)

// validateJSONSchema performs basic JSON schema validation using
// type checks. For full schema validation, integrate a proper schema
// validator library.
func validateJSONSchema(schema, data json.RawMessage) error {
	var schemaDef map[string]any
	if err := json.Unmarshal(schema, &schemaDef); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	var dataVal any
	if err := json.Unmarshal(data, &dataVal); err != nil {
		return fmt.Errorf("invalid data: %w", err)
	}

	return validateAgainstSchema(schemaDef, dataVal, "")
}

func validateAgainstSchema(schema map[string]any, data any, path string) error {
	dataType, _ := schema["type"].(string)
	if dataType == "" {
		return nil // No type constraint
	}

	switch dataType {
	case "object":
		obj, ok := data.(map[string]any)
		if !ok {
			return fmt.Errorf("expected object at %s, got %T", path, data)
		}
		props, _ := schema["properties"].(map[string]any)
		for key, propSchema := range props {
			if propData, exists := obj[key]; exists {
				propMap, _ := propSchema.(map[string]any)
				if err := validateAgainstSchema(propMap, propData, path+"."+key); err != nil {
					return err
				}
			}
		}
	case "array":
		arr, ok := data.([]any)
		if !ok {
			return fmt.Errorf("expected array at %s, got %T", path, data)
		}
		items, _ := schema["items"].(map[string]any)
		for i, item := range arr {
			if err := validateAgainstSchema(items, item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	case "string":
		if _, ok := data.(string); !ok {
			return fmt.Errorf("expected string at %s, got %T", path, data)
		}
	case "number":
		if _, ok := data.(float64); !ok {
			return fmt.Errorf("expected number at %s, got %T", path, data)
		}
	case "boolean":
		if _, ok := data.(bool); !ok {
			return fmt.Errorf("expected boolean at %s, got %T", path, data)
		}
	}

	// Check required fields for objects.
	if dataType == "object" {
		required, _ := schema["required"].([]any)
		obj, ok := data.(map[string]any)
		if !ok {
			return nil
		}
		for _, req := range required {
			reqStr, _ := req.(string)
			if _, exists := obj[reqStr]; !exists {
				return fmt.Errorf("missing required field '%s' at %s", reqStr, path)
			}
		}
	}

	return nil
}

// Ensure JSTransformer satisfies entity.Transformer.
var _ entity.Transformer = (*JSTransformer)(nil)

// SanitizeScript removes potentially dangerous patterns from JS scripts.
func SanitizeScript(script string) string {
	// Remove access to dangerous globals.
	dangerousPatterns := []string{
		"require", "process", "global", "__", "Buffer", "eval",
		"Function(", "setTimeout", "setInterval", "fetch",
		"importScripts", "XMLHttpRequest", "Worker",
	}
	sanitized := script
	for _, pattern := range dangerousPatterns {
		// Replace with safe stubs.
		sanitized = strings.ReplaceAll(sanitized, pattern, "_"+pattern+"_")
	}
	return sanitized
}
