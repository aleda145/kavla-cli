package sources

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/aleda145/kavla-cli/internal/auth"
)

type bigQueryAdapter struct {
	sourceName string
}

func (bigQueryAdapter) Type() string {
	return "bigquery"
}

func (bigQueryAdapter) StartupRequirements(src auth.SourceConfig) (*StartupRequirements, error) {
	return &StartupRequirements{
		StartupSQL: []string{
			"INSTALL bigquery FROM community",
			"LOAD bigquery",
		},
	}, nil
}

func (b *bigQueryAdapter) Init(ctx context.Context, reg RegistrationContext, name string, src auth.SourceConfig) error {
	b.sourceName = name
	connStr := src.Connection
	if !strings.Contains(connStr, "project=") {
		connStr = "project=" + connStr
	}

	attachSQL := fmt.Sprintf("ATTACH '%s' AS \"%s\" (TYPE bigquery, READ_ONLY)", connStr, name)
	if reg.ExecSQL != nil {
		if err := reg.ExecSQL(fmt.Sprintf("Attach BigQuery source '%s'", name), attachSQL); err != nil {
			return err
		}
	} else if _, err := reg.DB.ExecContext(ctx, attachSQL); err != nil {
		return fmt.Errorf("ATTACH bigquery: %w", err)
	}
	reg.Logf("Source '%s' (bigquery): attached '%s' in READ_ONLY mode\n", name, connStr)
	return nil
}

func (b *bigQueryAdapter) ListTables(ctx context.Context, db *sql.DB) ([]string, error) {
	return listTablesFromCatalog(ctx, db, func(databaseName, schemaName, tableName string) (string, bool) {
		if databaseName != b.sourceName {
			return "", false
		}
		if schemaName != "" && schemaName != "main" {
			return databaseName + "." + schemaName + "." + tableName, true
		}
		return databaseName + "." + tableName, true
	})
}

func (bigQueryAdapter) PreparePreviewSQL(sourceName, sql string, limit int) (string, error) {
	escapedSQL := strings.ReplaceAll(sql, "'", "''")
	return fmt.Sprintf("SELECT * FROM bigquery_query('%s', '%s') LIMIT %d", sourceName, escapedSQL, limit), nil
}

// TODO
// something like:
// SELECT column_name, is_nullable, data_type
// FROM `bigquery-public-data.austin_311.INFORMATION_SCHEMA.COLUMNS`
// WHERE table_name="311_request"
// is probably better

func (b *bigQueryAdapter) DescribeTableSQL(tableRef string) (string, bool, error) {
	tablePath, ok := sourceTablePath(b.sourceName, tableRef)
	if !ok {
		return "", false, nil
	}
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", backtickQualified(tablePath))
	wrappedSQL, err := b.PreparePreviewSQL(b.sourceName, query, 10000)
	if err != nil {
		return "", false, err
	}
	return fmt.Sprintf("DESCRIBE %s", wrappedSQL), true, nil
}

// TODO querying the information schema is probably also better here

func (b *bigQueryAdapter) SourceStatsSQL(tableRef string) (string, bool, error) {
	tablePath, ok := sourceTablePath(b.sourceName, tableRef)
	if !ok {
		return "", false, nil
	}
	query := fmt.Sprintf("SELECT COUNT(*) AS row_count FROM %s", backtickQualified(tablePath))
	wrappedSQL, err := b.PreparePreviewSQL(b.sourceName, query, 10000)
	if err != nil {
		return "", false, err
	}
	return fmt.Sprintf("SELECT row_count FROM (%s)", wrappedSQL), true, nil
}

func backtickQualified(parts []string) string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, "`"+strings.ReplaceAll(part, "`", "``")+"`")
	}
	return strings.Join(quoted, ".")
}

var _ Adapter = (*bigQueryAdapter)(nil)
