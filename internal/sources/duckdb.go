package sources

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/aleda145/kavla-cli/internal/auth"
)

type duckDBAdapter struct {
	sourceName string
}

func (duckDBAdapter) Type() string {
	return "duckdb"
}

func (duckDBAdapter) StartupRequirements(src auth.SourceConfig) (*StartupRequirements, error) {
	absPath, err := filepath.Abs(src.Connection)
	if err != nil {
		return nil, fmt.Errorf("resolve duckdb source path: %w", err)
	}
	return &StartupRequirements{
		AllowedPaths: []string{filepath.Clean(absPath)},
	}, nil
}

func (d *duckDBAdapter) Init(ctx context.Context, reg RegistrationContext, name string, src auth.SourceConfig) error {
	d.sourceName = name
	sql := fmt.Sprintf("ATTACH '%s' AS \"%s\" (READ_ONLY)", src.Connection, name)
	if reg.ExecSQL != nil {
		if err := reg.ExecSQL(fmt.Sprintf("Attach DuckDB source '%s'", name), sql); err != nil {
			return err
		}
	} else if _, err := reg.DB.ExecContext(ctx, sql); err != nil {
		return fmt.Errorf("ATTACH duckdb: %w", err)
	}
	reg.Logf("Source '%s' (duckdb): attached '%s' with local file access limited to that path\n", name, src.Connection)
	return nil
}

func (d *duckDBAdapter) ListTables(ctx context.Context, db *sql.DB) ([]string, error) {
	return listTablesFromCatalog(ctx, db, func(databaseName, schemaName, tableName string) (string, bool) {
		if databaseName != d.sourceName {
			return "", false
		}
		if schemaName != "" && schemaName != "main" {
			return databaseName + "." + schemaName + "." + tableName, true
		}
		return databaseName + "." + tableName, true
	})
}

func (duckDBAdapter) PreparePreviewSQL(sourceName, sql string, limit int) (string, error) {
	return DefaultPreviewSQL(sourceName, sql, limit)
}

func (d *duckDBAdapter) DescribeTableSQL(tableRef string) (string, bool, error) {
	tablePath, ok := sourceTablePath(d.sourceName, tableRef)
	if !ok {
		return "", false, nil
	}
	return fmt.Sprintf("DESCRIBE %s", doubleQuotedQualified(append([]string{d.sourceName}, tablePath...))), true, nil
}

func (d *duckDBAdapter) SourceStatsSQL(tableRef string) (string, bool, error) {
	tablePath, ok := sourceTablePath(d.sourceName, tableRef)
	if !ok {
		return "", false, nil
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM %s", doubleQuotedQualified(append([]string{d.sourceName}, tablePath...))), true, nil
}

func doubleQuotedQualified(parts []string) string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, `"`+strings.ReplaceAll(part, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, ".")
}

var _ Adapter = (*duckDBAdapter)(nil)
