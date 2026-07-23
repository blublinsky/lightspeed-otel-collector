package postgresexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// pool abstracts pgxpool.Pool for testability.
type pool interface {
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
	Ping(ctx context.Context) error
	Stat() *pgxpool.Stat
	Close()
}

type postgresExporter struct {
	config    *Config
	pool      pool
	logger    *zap.Logger
	telemetry *telemetry
	ts        component.TelemetrySettings
}

func (e *postgresExporter) start(ctx context.Context, _ component.Host) error {
	if err := e.pool.Ping(ctx); err != nil {
		e.pool.Close()
		return fmt.Errorf("failed to ping postgres: %w", err)
	}

	stat := e.pool.Stat()
	e.logger.Info("connected to postgres",
		zap.Int32("max_conns", stat.MaxConns()),
	)

	if err := e.telemetry.registerPoolMetrics(e.ts, e.pool); err != nil {
		return fmt.Errorf("failed to register pool metrics: %w", err)
	}
	return nil
}

func (e *postgresExporter) shutdown(_ context.Context) error {
	if e.pool != nil {
		e.pool.Close()
	}
	return nil
}

// consumeLogs is the hot path — called by the Collector for every batch of
// logs that flows through the pipeline. Each batch is written in a single
// multi-value INSERT so either the entire batch is committed or none of it is.
//
// Per log record, extracts:
//   - agentic_run_id: from log attribute "agenticrun.uid" (standard UUID with hyphens)
//   - phase: from log attribute "agenticrun.phase"
//   - timestamp: from TimeUnixNano (falls back to ObservedTimestamp, then now)
//   - event: from log record attributes (key: "event")
//   - body: log record body serialized as JSONB
func (e *postgresExporter) consumeLogs(ctx context.Context, ld plog.Logs) error {
	recordCount := ld.LogRecordCount()
	if recordCount == 0 {
		return nil
	}

	e.telemetry.recordBatchSize(ctx, recordCount)
	insertStart := time.Now()

	// Collect all row values into a flat args slice and build a multi-value
	// INSERT: INSERT INTO t (cols) VALUES ($1,$2,$3,$4,$5), ($6,$7,$8,$9,$10), ...
	// Records missing "agenticrun.uid" are skipped (unqueryable without a run ID).
	args := make([]interface{}, 0, recordCount*5)
	skipped := 0
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)

				// "agenticrun.uid" follows OTel semantic convention (dotted namespace).
				// Mapped to DB column "agentic_run_id".
				agenticRunID := ""
				if v, ok := lr.Attributes().Get("agenticrun.uid"); ok {
					agenticRunID = v.AsString()
				}
				if agenticRunID == "" {
					skipped++
					continue
				}

				ts := lr.Timestamp().AsTime()
				if ts.IsZero() {
					ts = lr.ObservedTimestamp().AsTime()
				}
				if ts.IsZero() {
					ts = time.Now()
				}

				phase := ""
				if v, ok := lr.Attributes().Get("agenticrun.phase"); ok {
					phase = v.AsString()
				}

				event := ""
				if v, ok := lr.Attributes().Get("event"); ok {
					event = v.AsString()
				}

				body, err := json.Marshal(lr.Body().AsRaw())
				if err != nil {
					raw := lr.Body().AsString()
					body, _ = json.Marshal(map[string]string{"raw": raw})
				}

				args = append(args, agenticRunID, phase, ts, event, body)
			}
		}
	}

	if skipped > 0 {
		e.logger.Warn("skipped log records missing agenticrun.uid attribute",
			zap.Int("skipped", skipped),
			zap.Int("inserted", len(args)/5),
		)
	}

	inserted := len(args) / 5
	if inserted == 0 {
		return nil
	}

	// Build VALUES placeholders: ($1,$2,$3,$4,$5), ($6,$7,$8,$9,$10), ...
	tuples := make([]string, 0, inserted)
	for i := 0; i < inserted; i++ {
		base := i*5 + 1
		tuples = append(tuples, fmt.Sprintf("($%d,$%d,$%d,$%d,$%d)", base, base+1, base+2, base+3, base+4))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (agentic_run_id, phase, timestamp, event, body) VALUES %s",
		e.config.qualifiedTable(),
		strings.Join(tuples, ","),
	)

	_, err := e.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert log records: %w", err)
	}

	e.telemetry.recordInsert(ctx, insertStart)
	return nil
}
