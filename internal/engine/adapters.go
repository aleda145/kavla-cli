package engine

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/aleda145/kavla-cli/internal/sources"
)

func (e *Engine) getAdapterForType(executionEngine string) (sources.Adapter, error) {
	factory, ok := e.adapterRegistry[executionEngine]
	if !ok {
		return nil, fmt.Errorf("unsupported execution engine: %s. Supported engines are: duckdb, directory, bigquery", executionEngine)
	}
	return factory(), nil
}

func (e *Engine) PreparePreviewSQL(executionEngine, sourceName, sql string, limit int) (string, error) {
	executionEngine = strings.TrimSpace(executionEngine)
	sourceName = strings.TrimSpace(sourceName)

	if executionEngine != "" {
		if sourceName == "" {
			return "", fmt.Errorf("source-native preview mode requires sourceName")
		}
		adapter, err := e.getAdapterForType(executionEngine)
		if err != nil {
			return "", err
		}
		return adapter.PreparePreviewSQL(sourceName, sql, limit)
	}
	return sources.DefaultPreviewSQL(sourceName, sql, limit)
}

func (e *Engine) DescribeTable(ctx context.Context, tableRef string) ([]map[string]string, error) {
	if err := e.ensureDeferredTables(ctx, tableRef); err != nil {
		return nil, err
	}

	describeSQL, err := e.describeTableSQL(tableRef)
	if err != nil {
		return nil, err
	}

	rows, err := e.db.QueryContext(ctx, describeSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []map[string]string
	for rows.Next() {
		var columnName, columnType, nullable, key, defaultValue, extra sql.NullString
		if err := rows.Scan(&columnName, &columnType, &nullable, &key, &defaultValue, &extra); err != nil {
			return nil, err
		}
		columns = append(columns, map[string]string{
			"name": columnName.String,
			"type": columnType.String,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columns, nil
}

func (e *Engine) GetSourceStats(ctx context.Context, tableRef string) (map[string]int64, error) {
	if err := e.ensureDeferredTables(ctx, tableRef); err != nil {
		return nil, err
	}

	var rowCount int64
	statsSQL, err := e.sourceStatsSQL(tableRef)
	if err != nil {
		return nil, err
	}

	if err := e.db.QueryRowContext(ctx, statsSQL).Scan(&rowCount); err != nil {
		return nil, err
	}

	return map[string]int64{
		"rowCount": rowCount,
	}, nil
}

func (e *Engine) describeTableSQL(tableRef string) (string, error) {
	for _, adapter := range e.adapters {
		describeSQL, ok, err := adapter.DescribeTableSQL(tableRef)
		if err != nil {
			return "", err
		}
		if ok {
			return describeSQL, nil
		}
	}
	return fmt.Sprintf("DESCRIBE %s", tableRef), nil
}

func (e *Engine) sourceStatsSQL(tableRef string) (string, error) {
	for _, adapter := range e.adapters {
		statsSQL, ok, err := adapter.SourceStatsSQL(tableRef)
		if err != nil {
			return "", err
		}
		if ok {
			return statsSQL, nil
		}
	}
	return fmt.Sprintf("SELECT count(*) FROM %s", tableRef), nil
}
