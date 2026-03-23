package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/aleda145/kavla-cli/internal/sources"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	_ "github.com/duckdb/duckdb-go/v2"
)

type Engine struct {
	db              *sql.DB
	logf            func(string, ...interface{})
	adapterRegistry map[string]sources.AdapterFactory
	adapters        map[string]sources.Adapter
}

type startupSecurityPlan struct {
	AllowedDirectories []string
	AllowedPaths       []string
	StartupSQL         []string
	SourceOrder        []string
}

// New creates a new in-memory DuckDB engine and initializes all configured sources.
func New(sourceConfigs map[string]auth.SourceConfig, logf func(string, ...interface{})) (*Engine, error) {
	if logf == nil {
		logf = log.Printf
	}

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("failed to open in-memory DuckDB: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping DuckDB: %w", err)
	}
	db.SetMaxOpenConns(5)

	e := &Engine{
		db:              db,
		logf:            logf,
		adapterRegistry: sources.BuiltIn(),
		adapters:        make(map[string]sources.Adapter),
	}

	if err := e.initializeStartup(context.Background(), sourceConfigs); err != nil {
		_ = db.Close()
		return nil, err
	}

	return e, nil
}

func (e *Engine) initializeStartup(ctx context.Context, sourceConfigs map[string]auth.SourceConfig) error {
	plan, err := buildStartupSecurityPlan(sourceConfigs)
	if err != nil {
		return err
	}

	for _, sql := range plan.StartupSQL {
		if err := e.execStartupSQL(ctx, sql); err != nil {
			return err
		}
	}

	for _, name := range plan.SourceOrder {
		src := sourceConfigs[name]
		if err := e.initSource(ctx, name, src); err != nil {
			return err
		}
	}

	sql := fmt.Sprintf("SET allowed_directories = %s", sqlStringList(plan.AllowedDirectories))
	if err := e.execStartupSQL(ctx, sql); err != nil {
		return err
	}

	sql = fmt.Sprintf("SET allowed_paths = %s", sqlStringList(plan.AllowedPaths))
	if err := e.execStartupSQL(ctx, sql); err != nil {
		return err
	}

	if err := e.execStartupSQL(ctx, "SET lock_configuration = true"); err != nil {
		return err
	}
	e.logf("DuckDB configuration locked\n")

	e.logf("DuckDB startup ready\n")
	return nil
}

func (e *Engine) initSource(ctx context.Context, name string, src auth.SourceConfig) error {
	adapter, err := e.getAdapterForType(src.Type)
	if err != nil {
		return err
	}
	if err := adapter.Init(ctx, sources.RegistrationContext{
		DB:   e.db,
		Logf: e.logf,
		ExecSQL: func(label, sql string) error {
			return e.execStartupSQL(ctx, sql)
		},
	}, name, src); err != nil {
		return err
	}
	e.adapters[name] = adapter
	return nil
}

func (e *Engine) execStartupSQL(ctx context.Context, sql string) error {
	if _, err := e.db.ExecContext(ctx, sql); err != nil {
		return err
	}
	return nil
}

func buildStartupSecurityPlan(sourceConfigs map[string]auth.SourceConfig) (*startupSecurityPlan, error) {
	plan := &startupSecurityPlan{}
	dirSet := make(map[string]struct{})
	pathSet := make(map[string]struct{})
	startupSQLSet := make(map[string]struct{})
	adapters := sources.BuiltIn()

	for name, src := range sourceConfigs {
		plan.SourceOrder = append(plan.SourceOrder, name)
		factory, ok := adapters[src.Type]
		if !ok {
			return nil, fmt.Errorf("unsupported execution engine: %s. Supported engines are: duckdb, directory, bigquery", src.Type)
		}
		adapter := factory()
		requirements, err := adapter.StartupRequirements(src)
		if err != nil {
			return nil, fmt.Errorf("source '%s': %w", name, err)
		}
		for _, dir := range requirements.AllowedDirectories {
			dirSet[dir] = struct{}{}
		}
		for _, path := range requirements.AllowedPaths {
			pathSet[path] = struct{}{}
		}
		for _, sql := range requirements.StartupSQL {
			if _, exists := startupSQLSet[sql]; exists {
				continue
			}
			startupSQLSet[sql] = struct{}{}
			plan.StartupSQL = append(plan.StartupSQL, sql)
		}
	}

	for dir := range dirSet {
		plan.AllowedDirectories = append(plan.AllowedDirectories, dir)
	}
	for path := range pathSet {
		plan.AllowedPaths = append(plan.AllowedPaths, path)
	}

	slices.Sort(plan.SourceOrder)
	slices.Sort(plan.AllowedDirectories)
	slices.Sort(plan.AllowedPaths)

	return plan, nil
}

func sqlStringList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, "'"+strings.ReplaceAll(value, "'", "''")+"'")
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func (e *Engine) Query(ctx context.Context, query string) (array.RecordReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := e.ensureDeferredTables(ctx, query); err != nil {
		return nil, err
	}

	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	fields := make([]arrow.Field, len(colTypes))
	for i, ct := range colTypes {
		fields[i] = arrow.Field{Name: ct.Name(), Type: getArrowType(ct.DatabaseTypeName())}
	}
	schema := arrow.NewSchema(fields, nil)

	mem := memory.NewGoAllocator()
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	values := make([]interface{}, len(colTypes))
	scanArgs := make([]interface{}, len(colTypes))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		for i, val := range values {
			if val == nil {
				b.Field(i).AppendNull()
				continue
			}

			switch b.Field(i).Type().(type) {
			case *arrow.Int64Type:
				var v int64
				switch t := val.(type) {
				case int64:
					v = t
				case int:
					v = int64(t)
				case int32:
					v = int64(t)
				case int16:
					v = int64(t)
				case int8:
					v = int64(t)
				case float64:
					v = int64(t)
				default:
					v = 0
				}
				b.Field(i).(*array.Int64Builder).Append(v)

			case *arrow.Float64Type:
				var v float64
				switch t := val.(type) {
				case float64:
					v = t
				case float32:
					v = float64(t)
				default:
					v = 0.0
				}
				b.Field(i).(*array.Float64Builder).Append(v)

			case *arrow.StringType:
				var s string
				switch v := val.(type) {
				case string:
					s = v
				case []byte:
					s = string(v)
				case time.Time:
					s = v.Format(time.RFC3339)
				default:
					s = fmt.Sprintf("%v", v)
				}
				b.Field(i).(*array.StringBuilder).Append(s)

			default:
				b.Field(i).(*array.StringBuilder).Append(fmt.Sprintf("%v", val))
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	rec := b.NewRecord()
	return array.NewRecordReader(schema, []arrow.Record{rec})
}

func (e *Engine) GetTables(sourceName string) ([]string, error) {
	if strings.TrimSpace(sourceName) == "" {
		return nil, fmt.Errorf("source name is required")
	}

	adapter, ok := e.adapters[sourceName]
	if !ok {
		return nil, fmt.Errorf("unknown source: %s", sourceName)
	}

	tables, err := adapter.ListTables(context.Background(), e.db)
	if err != nil {
		return nil, err
	}

	log.Printf("[Engine] GetTables found %d tables for source %s", len(tables), sourceName)
	return tables, nil
}

func (e *Engine) Close() error {
	return e.db.Close()
}

func (e *Engine) ensureDeferredTables(ctx context.Context, query string) error {
	for _, adapter := range e.adapters {
		materializer, ok := adapter.(sources.DeferredTableMaterializer)
		if !ok {
			continue
		}
		if err := materializer.MaterializeReferencedTables(ctx, query, func(sql string) error {
			_, err := e.db.ExecContext(ctx, sql)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

func getArrowType(dbType string) arrow.DataType {
	switch dbType {
	case "BIGINT", "INTEGER", "INT", "INT8", "INT4":
		return arrow.PrimitiveTypes.Int64
	case "DOUBLE", "FLOAT", "FLOAT8":
		return arrow.PrimitiveTypes.Float64
	default:
		return arrow.BinaryTypes.String
	}
}
