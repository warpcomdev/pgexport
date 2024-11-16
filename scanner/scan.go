package scanner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Metric define los atributos de una métrica
type Metric struct {
	Timestamp time.Time
	Name      string
	Value     float64
}

// Writer es un sink de métricas
type Writer interface {
	Write(ctx context.Context, logger *slog.Logger, labels map[string]string, metrics []Metric) error
}

// WriterFunc es un adaptador para la interfaz Writer
type WriterFunc func(ctx context.Context, logger *slog.Logger, laels map[string]string, metrics []Metric) error

// Write implementa Writer
func (f WriterFunc) Write(ctx context.Context, logger *slog.Logger, labels map[string]string, metrics []Metric) error {
	_ = Writer(f) // Make sure WriterFunc implements Writer
	return f(ctx, logger, labels, metrics)
}

// scanner es un sink de filas
type scanner interface {
	Scan(ctx context.Context, logger *slog.Logger, rows pgx.Rows) error
}

// scannerFunc es un adaptador para la interfaz Scanner
type scannerFunc func(ctx context.Context, logger *slog.Logger, rows pgx.Rows) error

// Scan implementa Scanner
func (f scannerFunc) Scan(ctx context.Context, logger *slog.Logger, rows pgx.Rows) error {
	_ = scanner(f) // Make sure scannerFunc implements scanner
	return f(ctx, logger, rows)
}

// doQuery ejecua una query y envía todas las filas al scanner
func doQuery(ctx context.Context, logger *slog.Logger, conn *pgx.Conn, query string, scanner scanner) error {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		logger.Error(err.Error(), "op", "query", "query", query)
		return err
	}
	defer rows.Close()
	for rows.Next() {
		err = scanner.Scan(ctx, logger, rows)
		if err != nil {
			logger.Error(err.Error(), "op", "scan")
			return err
		}
	}
	if rows.Err() != nil {
		logger.Error(err.Error(), "op", "error")
		return rows.Err()
	}
	return nil
}

// dbMetrics recopila métricas globales de las bases de datos
func dbMetrics(ctx context.Context, logger *slog.Logger, conn *pgx.Conn, w Writer) ([]string, error) {
	query := "SELECT datname, pg_database_size(datname) FROM pg_database"
	dbnames := make([]string, 0, 16)
	scanner := func(ctx context.Context, logger *slog.Logger, rows pgx.Rows) error {
		var database string
		var value int64
		if err := rows.Scan(&database, &value); err != nil {
			return err
		}
		logger.Debug("Scanned database size", "database", database, "size", value)
		labels := map[string]string{"database": database}
		metrics := []Metric{
			{
				Timestamp: time.Now(),
				Name:      "database_size_bytes",
				Value:     float64(value),
			},
		}
		dbnames = append(dbnames, database)
		return w.Write(ctx, logger, labels, metrics)
	}
	if err := doQuery(ctx, logger, conn, query, scannerFunc(scanner)); err != nil {
		return nil, err
	}
	return dbnames, nil
}

// hasTimescale finds out if a database has timescale extension
func hasTimescale(ctx context.Context, logger *slog.Logger, conn *pgx.Conn) (bool, error) {
	extVersion := ""
	query := "SELECT extversion FROM pg_extension where extname = 'timescaledb'"
	scanner := func(ctx context.Context, logger *slog.Logger, rows pgx.Rows) error {
		return rows.Scan(&extVersion)
	}
	if err := doQuery(ctx, logger, conn, query, scannerFunc(scanner)); err != nil {
		return false, err
	}
	if extVersion == "" {
		logger.Debug("Database does not have timescale extension")
		return false, nil
	}
	logger.Debug("Database has timescale extension", "version", extVersion)
	return true, nil
}

// tableMetrics recopila métricas individuales de las tablas
func tableMetrics(ctx context.Context, logger *slog.Logger, conn *pgx.Conn, w Writer, threshold int64) error {
	var query string
	ts, err := hasTimescale(ctx, logger, conn)
	if err != nil {
		return err
	}
	if ts {
		query = `
		SELECT
			CASE WHEN t2.hypertable_name IS NULL THEN 0 ELSE 1 END as is_hypertable,
			coalesce(t2.hypertable_schema, t1.table_schema) AS table_schema,
			coalesce(t2.hypertable_name, t1.table_name) as table_name,
			SUM(pg_total_relation_size(concat(quote_ident(t1.table_schema), '.', quote_ident(t1.table_name)))) as total_size,
			SUM(pg_relation_size(concat(quote_ident(t1.table_schema), '.', quote_ident(t1.table_name)))) as relation_size
		FROM information_schema.tables t1
		LEFT JOIN timescaledb_information.chunks t2
		ON t1.table_name=t2.chunk_name AND t1.table_schema=t2.chunk_schema
		GROUP BY 1, 2, 3
		`
	} else {
		query = `
		SELECT
			0 AS is_hypertable,
			t1.table_schema AS table_schema,
			t1.table_name as table_name,
			SUM(pg_total_relation_size(concat(quote_ident(t1.table_schema), '.', quote_ident(t1.table_name)))) as total_size,
			SUM(pg_relation_size(concat(quote_ident(t1.table_schema), '.', quote_ident(t1.table_name)))) as relation_size
		FROM information_schema.tables t1
		GROUP BY 1, 2, 3
		`
	}
	scanner := func(ctx context.Context, logger *slog.Logger, rows pgx.Rows) error {
		var isHypertable int
		var schema string
		var name string
		var tot_size int64
		var rel_size int64
		if err := rows.Scan(&isHypertable, &schema, &name, &tot_size, &rel_size); err != nil {
			return err
		}
		if tot_size < threshold {
			return nil
		}
		kind := "rel"
		if isHypertable > 0 {
			kind = "ht"
		}
		labels := map[string]string{
			"schema": schema,
			"name":   name,
			"kind":   kind,
		}
		now := time.Now()
		metrics := []Metric{
			{
				Timestamp: now,
				Name:      "table_is_hypertable",
				Value:     float64(isHypertable),
			},
			{
				Timestamp: now,
				Name:      "table_size_bytes",
				Value:     float64(tot_size),
			},
			{
				Timestamp: now,
				Name:      "table_relation_size_bytes",
				Value:     float64(rel_size),
			},
			{
				Timestamp: now,
				Name:      "table_index_size_bytes",
				Value:     float64(tot_size - rel_size),
			},
		}
		return w.Write(ctx, logger, labels, metrics)
	}
	if err := doQuery(ctx, logger, conn, query, scannerFunc(scanner)); err != nil {
		return err
	}
	return nil
}

// Factory genera conexiones para bases de datos
type Factory interface {
	// Conect to the named Database
	Connect(ctx context.Context, logger *slog.Logger, database string) (*pgx.Conn, error)
	// Dispose of the connection to the database
	Dispose(ctx context.Context, logger *slog.Logger, conn *pgx.Conn, database string) error
}

type Config struct {
	InitialDB  string        `json:"initialDb"`
	Exceptions []string      `json:"exceptions"`
	Threshold  int64         `json:"threshold"`
	Pause      time.Duration `json:"pause"`
}

func Defaults() Config {
	return Config{
		InitialDB:  "postgres",
		Exceptions: []string{"template0", "template1", "postgres"},
		Threshold:  0,
		Pause:      0,
	}
}

func Scan(ctx context.Context, logger *slog.Logger, cfg Config, factory Factory, w Writer) error {
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("Scanning databases", "options", cfg)
	// Wrap this inside a closure, for deferring
	dbNames, err := func() ([]string, error) {
		conn, err := factory.Connect(ctx, logger, cfg.InitialDB)
		if err != nil {
			return nil, err
		}
		defer factory.Dispose(ctx, logger, conn, cfg.InitialDB)
		return dbMetrics(ctx, logger, conn, w)
	}()
	if err != nil {
		logger.Error(err.Error(), "op", "db_metrics")
		return err
	}
	logger.Info("Databases found", "count", len(dbNames))
	dbErrors := make([]error, 0, len(dbNames))
	for _, database := range dbNames {
		dbErrors = append(dbErrors, scanDatabase(ctx, logger, cfg, factory, w, database))
	}
	return errors.Join(dbErrors...)
}

func scanDatabase(ctx context.Context, logger *slog.Logger, cfg Config, factory Factory, w Writer, database string) error {
	if cfg.Exceptions != nil {
		for _, exc := range cfg.Exceptions {
			match, err := filepath.Match(exc, database)
			if err != nil {
				logger.Warn(err.Error(), "op", "match", "pattern", exc, "database", database)
			} else if match {
				logger.Info("skipping database", "database", database)
				return nil
			}
		}
	}
	dbLogger := logger.With("database", database)
	if cfg.Pause > 0 {
		dbLogger.Info("pausing before next scan", "pause", cfg.Pause.String())
		time.Sleep(cfg.Pause)
	}
	dbLogger.Info("scanning tables")
	dbWriter := WriterFunc(func(ctx context.Context, dbLogger *slog.Logger, labels map[string]string, metrics []Metric) error {
		labels["database"] = database
		return w.Write(ctx, dbLogger, labels, metrics)
	})
	// Wrap this inside a closure, for deferring
	err := func() error {
		conn, err := factory.Connect(ctx, dbLogger, database)
		if err != nil {
			return err
		}
		defer factory.Dispose(ctx, dbLogger, conn, database)
		return tableMetrics(ctx, dbLogger, conn, dbWriter, cfg.Threshold)
	}()
	if err != nil {
		dbLogger.Error(err.Error(), "op", "table_metrics")
	}
	return err
}

// TextWriter escribe las métricas en formato text-exposition a un writer
func TextWriter(w io.Writer, prefix string) Writer {
	return WriterFunc(func(ctx context.Context, logger *slog.Logger, labels map[string]string, metrics []Metric) error {
		var l strings.Builder
		sep := ""
		for k, v := range labels {
			// quoe and escape
			escaped, err := json.Marshal(v)
			if err != nil {
				logger.Error(err.Error(), "op", "marshal", "k", k, "v", v)
				return err
			}
			l.WriteString(sep)
			sep = ","
			l.WriteString(k)
			l.WriteString("=")
			l.Write(escaped)
		}
		for _, m := range metrics {
			if _, err := fmt.Fprintf(w, "%s%s{%s} %f %d\n", prefix, m.Name, l.String(), m.Value, m.Timestamp.UnixMilli()); err != nil {
				return err
			}
		}
		return nil
	})
}
