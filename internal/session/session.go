package session

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/aleda145/kavla-cli/internal/engine"
	"github.com/aleda145/kavla-cli/internal/runner"
)

type Client interface {
	SendJSON(msg map[string]interface{}) error
	SendResultData(shapeID string, format runner.ResultFormat, data []byte) error
	GetR2PresignedURL(shapeID string) (string, string, error)
}

type Session struct {
	sources  map[string]auth.SourceConfig
	engine   *engine.Engine
	logf     func(string, ...interface{})
	verbosef func(string, ...interface{})
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	queries  map[string]activeQuery
}

type activeQuery struct {
	cancel func()
	name   string
}

type SessionTransport struct {
	client   Client
	logf     func(string, ...interface{})
	verbosef func(string, ...interface{})
}

func New(sources map[string]auth.SourceConfig) *Session {
	if sources == nil {
		sources = make(map[string]auth.SourceConfig)
	}
	return &Session{
		sources: sources,
		queries: make(map[string]activeQuery),
	}
}

func (s *Session) SetLogger(logf func(string, ...interface{})) {
	s.logf = logf
}

func (s *Session) SetVerboseLogger(verbosef func(string, ...interface{})) {
	s.verbosef = verbosef
}

func (s *Session) log(format string, args ...interface{}) {
	if s.logf != nil {
		s.logf(format, args...)
	}
}

func (s *Session) Start() error {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	eng, err := engine.New(s.sources, s.logf)
	if err != nil {
		s.cancel()
		s.ctx = nil
		s.cancel = nil
		return err
	}
	s.engine = eng
	return nil
}

func (s *Session) Close() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Lock()
	for shapeID, query := range s.queries {
		query.cancel()
		delete(s.queries, shapeID)
	}
	s.mu.Unlock()
	if s.engine == nil {
		return nil
	}
	return s.engine.Close()
}

func (s *Session) SourceList() []map[string]interface{} {
	if s.engine != nil {
		statuses := s.engine.SourceStatuses()
		sourceList := make([]map[string]interface{}, 0, len(statuses))
		for _, status := range statuses {
			entry := map[string]interface{}{
				"name":      status.Name,
				"type":      status.Type,
				"available": status.Available,
			}
			if status.Error != "" {
				entry["error"] = status.Error
			}
			sourceList = append(sourceList, entry)
		}
		return sourceList
	}

	sourceList := make([]map[string]interface{}, 0, len(s.sources))
	for name, src := range s.sources {
		sourceList = append(sourceList, map[string]interface{}{
			"name":      name,
			"type":      src.Type,
			"available": true,
		})
	}
	return sourceList
}

func (s *Session) HandleQuery(client Client, req runner.QueryRequest) {
	queryLabel := req.QueryName
	if queryLabel == "" {
		queryLabel = req.ShapeId
	}

	if req.UserName != "" {
		s.log("\nQuery from %s (source '%s'):\n%s\n", req.UserName, req.SourceName, strings.TrimSpace(req.SQL))
	} else {
		s.log("\nQuery from source '%s':\n%s\n", req.SourceName, strings.TrimSpace(req.SQL))
	}

	queryCtx, cancel := context.WithCancel(s.ctx)
	s.mu.Lock()
	if existing, ok := s.queries[req.ShapeId]; ok {
		existing.cancel()
	}
	s.queries[req.ShapeId] = activeQuery{
		cancel: cancel,
		name:   queryLabel,
	}
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		delete(s.queries, req.ShapeId)
		s.mu.Unlock()
	}()

	result, err := runner.Execute(queryCtx, req, &SessionTransport{
		client:   client,
		logf:     s.logf,
		verbosef: s.verbosef,
	}, s.engine)
	if err != nil {
		if queryCtx.Err() != nil {
			s.log("Query cancelled for %s\n", queryLabel)
			return
		}
		s.log("Query failed: %v\n", err)
		_ = client.SendJSON(map[string]interface{}{
			"type":    "query_error",
			"error":   err.Error(),
			"shapeId": req.ShapeId,
		})
		return
	}

	s.log("%d rows sent, %d bytes to canvas\n", result.RowCount, result.DataSize)
}

func (s *Session) CancelQuery(shapeID string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	query, ok := s.queries[shapeID]
	if !ok {
		return "", false
	}

	query.cancel()
	delete(s.queries, shapeID)
	return query.name, true
}

func (s *Session) HandleGetTables(client Client, requestID, sourceName string) {
	if strings.TrimSpace(sourceName) == "" {
		_ = client.SendJSON(map[string]interface{}{
			"type":      "error",
			"requestId": requestID,
			"payload": map[string]string{
				"message": "failed to get tables: source name is required",
			},
		})
		return
	}

	tables, err := s.engine.GetTables(sourceName)
	if err != nil {
		s.log("Failed to get tables for source '%s': %v\n", sourceName, err)
		_ = client.SendJSON(map[string]interface{}{
			"type":      "error",
			"requestId": requestID,
			"payload": map[string]string{
				"message": fmt.Sprintf("failed to get tables: %v", err),
			},
		})
		return
	}

	_ = client.SendJSON(map[string]interface{}{
		"type":      "get_source_tables_response",
		"requestId": requestID,
		"payload": map[string]interface{}{
			"tables": tables,
		},
	})
}

func (s *Session) HandleGetSourceSchema(client Client, requestID, tableRef string) {
	columns, err := s.engine.DescribeTable(s.ctx, tableRef)
	if err != nil {
		s.log("Failed to get schema for table '%s': %v\n", tableRef, err)
		_ = client.SendJSON(map[string]interface{}{
			"type":      "error",
			"requestId": requestID,
			"payload": map[string]string{
				"message": fmt.Sprintf("failed to get schema: %v", err),
			},
		})
		return
	}

	_ = client.SendJSON(map[string]interface{}{
		"type":      "get_source_schema_response",
		"requestId": requestID,
		"payload": map[string]interface{}{
			"columns": columns,
		},
	})
}

func (s *Session) HandleGetSourceStats(client Client, requestID, tableRef string) {
	stats, err := s.engine.GetSourceStats(s.ctx, tableRef)
	if err != nil {
		s.log("Failed to get stats for table '%s': %v\n", tableRef, err)
		_ = client.SendJSON(map[string]interface{}{
			"type":      "error",
			"requestId": requestID,
			"payload": map[string]string{
				"message": fmt.Sprintf("failed to get source stats: %v", err),
			},
		})
		return
	}

	_ = client.SendJSON(map[string]interface{}{
		"type":      "get_source_stats_response",
		"requestId": requestID,
		"payload":   stats,
	})
}

func (t *SessionTransport) SendResultData(shapeID string, format runner.ResultFormat, data []byte) error {
	return t.client.SendResultData(shapeID, format, data)
}

func (t *SessionTransport) GetR2PresignedURL(shapeID string) (string, string, error) {
	return t.client.GetR2PresignedURL(shapeID)
}

func (t *SessionTransport) SendJSON(msg map[string]interface{}) error {
	return t.client.SendJSON(msg)
}

func (t *SessionTransport) Log(format string, args ...interface{}) {
	if t.logf != nil {
		t.logf(format, args...)
	}
}

func (t *SessionTransport) Verbose(format string, args ...interface{}) {
	if t.verbosef != nil {
		t.verbosef(format, args...)
	}
}
