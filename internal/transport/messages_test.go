package transport

import (
	"strings"
	"testing"
)

func TestParseQueryRequestMessageAllowsMissingExecutionEngine(t *testing.T) {
	req, err := ParseQueryRequestMessage(map[string]interface{}{
		"payload": map[string]interface{}{
			"sql":     "SELECT 1",
			"shapeId": "shape-1",
		},
	})
	if err != nil {
		t.Fatalf("ParseQueryRequestMessage returned error: %v", err)
	}

	if req.ExecutionEngine != "" {
		t.Fatalf("expected empty execution engine, got %q", req.ExecutionEngine)
	}
}

func TestParseQueryRequestMessageRequiresSourceNameForExecutionEngine(t *testing.T) {
	_, err := ParseQueryRequestMessage(map[string]interface{}{
		"payload": map[string]interface{}{
			"sql":             "SELECT 1",
			"shapeId":         "shape-1",
			"executionEngine": "bigquery",
		},
	})
	if err == nil {
		t.Fatal("expected an error when executionEngine is set without sourceName")
	}

	if !strings.Contains(err.Error(), "sourceName is required") {
		t.Fatalf("expected sourceName validation error, got %v", err)
	}
}
