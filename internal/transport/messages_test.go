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

func TestParseQueryRequestMessageParsesMountedFileSources(t *testing.T) {
	req, err := ParseQueryRequestMessage(map[string]interface{}{
		"payload": map[string]interface{}{
			"sql":     "SELECT * FROM orders",
			"shapeId": "shape-1",
			"mountedFileSources": []interface{}{
				map[string]interface{}{
					"sourceName":  "orders",
					"r2ObjectKey": "owner/room/orders.parquet",
					"fileName":    "orders.parquet",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ParseQueryRequestMessage returned error: %v", err)
	}

	if len(req.MountedFileSources) != 1 {
		t.Fatalf("expected 1 mounted file source, got %d", len(req.MountedFileSources))
	}

	mounted := req.MountedFileSources[0]
	if mounted.SourceName != "orders" || mounted.R2ObjectKey != "owner/room/orders.parquet" || mounted.FileName != "orders.parquet" {
		t.Fatalf("unexpected mounted file source: %+v", mounted)
	}
}
