package sources

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aleda145/kavla-cli/internal/auth"
)

type deferredTable struct {
	ref       string
	matchName string
	createSQL string
}

type directoryAdapter struct {
	logf          func(string, ...interface{})
	sourceName    string
	mu            sync.Mutex
	pendingTables map[string]deferredTable
}

func (*directoryAdapter) Type() string {
	return "directory"
}

func (*directoryAdapter) StartupRequirements(src auth.SourceConfig) (*StartupRequirements, error) {
	absDir, err := filepath.Abs(src.Connection)
	if err != nil {
		return nil, fmt.Errorf("resolve directory path: %w", err)
	}
	return &StartupRequirements{
		AllowedDirectories: []string{filepath.Clean(absDir)},
	}, nil
}

func (d *directoryAdapter) Init(ctx context.Context, reg RegistrationContext, name string, src auth.SourceConfig) error {
	absDir, err := filepath.Abs(src.Connection)
	if err != nil {
		return fmt.Errorf("resolve directory path: %w", err)
	}
	d.logf = reg.Logf
	d.sourceName = name
	d.pendingTables = make(map[string]deferredTable)

	schemaSQL := fmt.Sprintf("CREATE SCHEMA \"%s\"", name)
	if reg.ExecSQL != nil {
		if err := reg.ExecSQL(fmt.Sprintf("Create schema for directory source '%s'", name), schemaSQL); err != nil {
			return err
		}
	} else if _, err := reg.DB.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("CREATE SCHEMA: %w", err)
	}

	viewCount := 0
	err = filepath.WalkDir(absDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if entry.IsDir() {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(path))
		var viewSQL string

		relPath, err := filepath.Rel(absDir, path)
		if err != nil {
			return nil
		}
		baseName := strings.TrimSuffix(relPath, filepath.Ext(relPath))

		switch ext {
		case ".parquet":
			viewSQL = fmt.Sprintf("CREATE VIEW \"%s\".\"%s\" AS SELECT * FROM '%s'", name, baseName, path)
		case ".csv":
			viewSQL = fmt.Sprintf("CREATE VIEW \"%s\".\"%s\" AS SELECT * FROM read_csv_auto('%s')", name, baseName, path)
		case ".json", ".ndjson":
			viewSQL = fmt.Sprintf("CREATE VIEW \"%s\".\"%s\" AS SELECT * FROM read_json_auto('%s')", name, baseName, path)
		default:
			return nil
		}

		tableRef := name + "." + baseName
		d.pendingTables[tableRef] = deferredTable{
			ref:       tableRef,
			matchName: baseName,
			createSQL: viewSQL,
		}
		viewCount++
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk directory '%s': %w", absDir, err)
	}

	reg.Logf("Source '%s' (directory): allowed '%s', %d file(s) queued for lazy loading\n", name, absDir, viewCount)
	return nil
}

func (d *directoryAdapter) ListTables(ctx context.Context, db *sql.DB) ([]string, error) {
	tables, err := listTablesFromCatalog(ctx, db, func(databaseName, schemaName, tableName string) (string, bool) {
		if schemaName != d.sourceName {
			return "", false
		}
		return schemaName + "." + tableName, true
	})
	if err != nil {
		return nil, err
	}

	tableSet := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		tableSet[table] = struct{}{}
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	for _, table := range d.pendingTables {
		if _, ok := tableSet[table.ref]; ok {
			continue
		}
		tables = append(tables, table.ref)
	}

	return tables, nil
}

func (*directoryAdapter) PreparePreviewSQL(sourceName, sql string, limit int) (string, error) {
	return DefaultPreviewSQL(sourceName, sql, limit)
}

func (d *directoryAdapter) DescribeTableSQL(tableRef string) (string, bool, error) {
	tablePath, ok := sourceTablePath(d.sourceName, tableRef)
	if !ok {
		return "", false, nil
	}
	return fmt.Sprintf("DESCRIBE %s", doubleQuotedQualified(append([]string{d.sourceName}, tablePath...))), true, nil
}

func (d *directoryAdapter) SourceStatsSQL(tableRef string) (string, bool, error) {
	tablePath, ok := sourceTablePath(d.sourceName, tableRef)
	if !ok {
		return "", false, nil
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM %s", doubleQuotedQualified(append([]string{d.sourceName}, tablePath...))), true, nil
}

func (d *directoryAdapter) MaterializeReferencedTables(ctx context.Context, query string, exec func(string) error) error {
	queryLower := strings.ToLower(query)
	d.mu.Lock()
	matches := make([]deferredTable, 0)
	for key, table := range d.pendingTables {
		if !strings.Contains(queryLower, strings.ToLower(table.matchName)) {
			continue
		}
		matches = append(matches, table)
		delete(d.pendingTables, key)
	}
	d.mu.Unlock()

	for _, table := range matches {
		if err := ctx.Err(); err != nil {
			return err
		}
		if d.logf != nil {
			d.logf("Lazily initializing view: %s\n", table.ref)
		}
		if err := exec(table.createSQL); err != nil && d.logf != nil {
			d.logf("Failed to lazy-load view %s: %v\n", table.ref, err)
		}
	}

	return nil
}

var _ Adapter = (*directoryAdapter)(nil)
var _ DeferredTableMaterializer = (*directoryAdapter)(nil)
