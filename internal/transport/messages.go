package transport

import (
	"fmt"
	"strings"

	"github.com/aleda145/kavla-cli/internal/runner"
)

type GetSourceTablesMessage struct {
	RequestID  string
	SourceName string
}

type GetSourceSchemaMessage struct {
	RequestID string
	TableRef  string
}

type GetSourceStatsMessage struct {
	RequestID string
	TableRef  string
}

type CancelQueryMessage struct {
	ShapeID   string
	QueryName string
}

func ParseGetSourceTablesMessage(msg map[string]interface{}) (*GetSourceTablesMessage, error) {
	requestID, _ := msg["requestId"].(string)
	payload, ok := msg["payload"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("missing payload")
	}

	sourceName, _ := payload["sourceName"].(string)
	return &GetSourceTablesMessage{
		RequestID:  requestID,
		SourceName: sourceName,
	}, nil
}

func ParseGetSourceSchemaMessage(msg map[string]interface{}) (*GetSourceSchemaMessage, error) {
	requestID, _ := msg["requestId"].(string)
	payload, ok := msg["payload"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("missing payload")
	}

	tableRef, ok := payload["tableRef"].(string)
	if !ok || tableRef == "" {
		return nil, fmt.Errorf("missing tableRef")
	}

	return &GetSourceSchemaMessage{
		RequestID: requestID,
		TableRef:  tableRef,
	}, nil
}

func ParseGetSourceStatsMessage(msg map[string]interface{}) (*GetSourceStatsMessage, error) {
	requestID, _ := msg["requestId"].(string)
	payload, ok := msg["payload"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("missing payload")
	}

	tableRef, ok := payload["tableRef"].(string)
	if !ok || tableRef == "" {
		return nil, fmt.Errorf("missing tableRef")
	}

	return &GetSourceStatsMessage{
		RequestID: requestID,
		TableRef:  tableRef,
	}, nil
}

func ParseCancelQueryMessage(msg map[string]interface{}) (*CancelQueryMessage, error) {
	payload, ok := msg["payload"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("missing payload")
	}

	shapeID, ok := payload["shapeId"].(string)
	if !ok || shapeID == "" {
		return nil, fmt.Errorf("missing shapeId")
	}

	queryName, _ := payload["queryName"].(string)

	return &CancelQueryMessage{
		ShapeID:   shapeID,
		QueryName: queryName,
	}, nil
}

func ParseQueryRequestMessage(msg map[string]interface{}) (runner.QueryRequest, error) {
	payload, ok := msg["payload"].(map[string]interface{})
	if !ok {
		return runner.QueryRequest{}, fmt.Errorf("missing payload")
	}

	sql, ok := payload["sql"].(string)
	if !ok || sql == "" {
		return runner.QueryRequest{}, fmt.Errorf("missing sql")
	}

	shapeID, ok := payload["shapeId"].(string)
	if !ok || shapeID == "" {
		return runner.QueryRequest{}, fmt.Errorf("missing shapeId")
	}

	sourceName, _ := payload["sourceName"].(string)
	executionEngine, _ := payload["executionEngine"].(string)
	sourceName = strings.TrimSpace(sourceName)
	executionEngine = strings.TrimSpace(executionEngine)
	if executionEngine != "" && sourceName == "" {
		return runner.QueryRequest{}, fmt.Errorf("sourceName is required when executionEngine is set")
	}
	queryName, _ := payload["queryName"].(string)
	userName, _ := payload["userName"].(string)

	return runner.QueryRequest{
		SQL:             sql,
		ShapeId:         shapeID,
		SourceName:      sourceName,
		ExecutionEngine: executionEngine,
		QueryName:       queryName,
		UserName:        userName,
	}, nil
}
