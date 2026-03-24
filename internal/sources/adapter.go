package sources

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/aleda145/kavla-cli/internal/auth"
)

type RegistrationContext struct {
	DB      *sql.DB
	Logf    func(string, ...interface{})
	ExecSQL func(label, sql string) error
}

type StartupRequirements struct {
	AllowedDirectories []string
	AllowedPaths       []string
	StartupSQL         []string
}

type DeferredTableMaterializer interface {
	MaterializeReferencedTables(ctx context.Context, query string, exec func(string) error) error
}

type Adapter interface {
	Type() string
	StartupRequirements(src auth.SourceConfig) (*StartupRequirements, error)
	Init(ctx context.Context, reg RegistrationContext, name string, src auth.SourceConfig) error
	ListTables(ctx context.Context, db *sql.DB) ([]string, error)
	PreparePreviewSQL(sourceName, sql string, limit int) (string, error)
	DescribeTableSQL(tableRef string) (string, bool, error)
	SourceStatsSQL(tableRef string) (string, bool, error)
}

type AdapterFactory func() Adapter

func BuiltIn() map[string]AdapterFactory {
	adapters := make(map[string]AdapterFactory, len(BuiltInDefinitions()))
	for _, definition := range BuiltInDefinitions() {
		adapters[definition.Type] = definition.NewAdapter
	}
	return adapters
}

func DefaultPreviewSQL(sourceName, sql string, limit int) (string, error) {
	return fmt.Sprintf("SELECT * FROM (%s) LIMIT %d", sql, limit), nil
}

func splitQualifiedTableRef(tableRef string) ([]string, bool) {
	rawParts := strings.Split(tableRef, ".")
	parts := make([]string, 0, len(rawParts))
	for _, rawPart := range rawParts {
		part := strings.TrimSpace(rawPart)
		if len(part) >= 2 && part[0] == '"' && part[len(part)-1] == '"' {
			part = strings.ReplaceAll(part[1:len(part)-1], `""`, `"`)
		}
		if part == "" {
			return nil, false
		}
		parts = append(parts, part)
	}

	if len(parts) < 2 {
		return nil, false
	}

	return parts, true
}

func sourceTablePath(sourceName, tableRef string) ([]string, bool) {
	parts, ok := splitQualifiedTableRef(tableRef)
	if !ok || parts[0] != sourceName {
		return nil, false
	}
	return parts[1:], true
}

func listTablesFromCatalog(ctx context.Context, db *sql.DB, format func(databaseName, schemaName, tableName string) (string, bool)) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW ALL TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	dbIdx, schemaIdx, nameIdx := -1, -1, -1
	for i, col := range cols {
		switch col {
		case "database":
			dbIdx = i
		case "schema":
			schemaIdx = i
		case "name":
			nameIdx = i
		}
	}

	if nameIdx == -1 {
		return nil, fmt.Errorf("could not find 'name' column in SHOW ALL TABLES output")
	}

	var tables []string
	for rows.Next() {
		values := make([]interface{}, len(cols))
		scanArgs := make([]interface{}, len(cols))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		databaseName := ""
		schemaName := ""
		if dbIdx >= 0 {
			databaseName = fmt.Sprintf("%v", values[dbIdx])
		}
		if schemaIdx >= 0 {
			schemaName = fmt.Sprintf("%v", values[schemaIdx])
		}

		tableName := fmt.Sprintf("%v", values[nameIdx])
		tableRef, ok := format(databaseName, schemaName, tableName)
		if !ok {
			continue
		}
		tables = append(tables, tableRef)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}
