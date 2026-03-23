package runner

import (
	"context"
	"strings"
	"testing"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/aleda145/kavla-cli/internal/engine"
)

type testTransport struct {
	verbose []string
}

func (t *testTransport) SendResultData(shapeId string, format ResultFormat, data []byte) error {
	return nil
}

func (t *testTransport) GetR2PresignedURL(shapeId string) (string, string, error) {
	return "", "", nil
}

func (t *testTransport) SendJSON(msg map[string]interface{}) error {
	return nil
}

func (t *testTransport) Log(format string, args ...interface{}) {
}

func (t *testTransport) Verbose(format string, args ...interface{}) {
	t.verbose = append(t.verbose, strings.TrimSpace(format))
}

func newTestEngine(t *testing.T) *engine.Engine {
	t.Helper()

	eng, err := engine.New(map[string]auth.SourceConfig{}, nil)
	if err != nil {
		t.Fatalf("engine.New returned error: %v", err)
	}

	t.Cleanup(func() {
		_ = eng.Close()
	})

	return eng
}

func TestPreparePreviewSQLUsesFederatedPreviewWhenExecutionEngineMissing(t *testing.T) {
	eng := newTestEngine(t)

	wrappedSQL, err := preparePreviewSQL(QueryRequest{
		SQL:     "SELECT 1;",
		ShapeId: "shape-1",
	}, eng, 50)
	if err != nil {
		t.Fatalf("preparePreviewSQL returned error: %v", err)
	}

	expected := "SELECT * FROM (SELECT 1) LIMIT 50"
	if wrappedSQL != expected {
		t.Fatalf("expected %q, got %q", expected, wrappedSQL)
	}
}

func TestPreparePreviewSQLUsesAdapterWrappingWhenExecutionEngineAndSourceNameAreSet(t *testing.T) {
	eng := newTestEngine(t)

	wrappedSQL, err := preparePreviewSQL(QueryRequest{
		SQL:             "SELECT * FROM analytics.events",
		ExecutionEngine: "bigquery",
		SourceName:      "analytics",
		ShapeId:         "shape-1",
	}, eng, 50)
	if err != nil {
		t.Fatalf("preparePreviewSQL returned error: %v", err)
	}

	if !strings.Contains(wrappedSQL, "bigquery_query('analytics'") {
		t.Fatalf("expected BigQuery adapter wrapper, got %q", wrappedSQL)
	}
}

func TestPreparePreviewSQLRequiresSourceNameWhenExecutionEngineIsSet(t *testing.T) {
	eng := newTestEngine(t)

	_, err := preparePreviewSQL(QueryRequest{
		SQL:             "SELECT 1",
		ExecutionEngine: "bigquery",
		ShapeId:         "shape-1",
	}, eng, 50)
	if err == nil {
		t.Fatal("expected an error when executionEngine is set without sourceName")
	}

	if !strings.Contains(err.Error(), "source-native preview mode requires sourceName") {
		t.Fatalf("expected source-native validation error, got %v", err)
	}
}

func TestExecuteRunsFederatedPreviewThroughDuckDBWhenExecutionEngineMissing(t *testing.T) {
	eng := newTestEngine(t)
	transport := &testTransport{}

	result, err := Execute(context.Background(), QueryRequest{
		SQL:     "SELECT 1 AS value UNION ALL SELECT 2 AS value",
		ShapeId: "shape-1",
	}, transport, eng)
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	if result.RowCount != 2 {
		t.Fatalf("expected 2 rows, got %d", result.RowCount)
	}
}
