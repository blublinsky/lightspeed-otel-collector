# PostgreSQL Exporter

Custom OTel Collector exporter that writes OTLP log records to PostgreSQL. See parent spec `ols/.ai/spec/what/templog.md` for schema DDL and cross-repo requirements.

## Behavioral Rules

### Core Functionality

1. The exporter receives batches of OTLP log records from the Collector pipeline.
2. For each log record, the exporter extracts:
   - **`trace_id`** — from the log record's trace context (`TraceID` field, 32-char hex string). This is the AgenticRun `metadata.uid` with hyphens stripped.
   - **`timestamp`** — from the log record's `TimeUnixNano` field, converted to `TIMESTAMPTZ`.
   - **`event`** — from the log record's attributes (key: `event`). This is the event discriminator (e.g., `audit.agenticrun.received`, `audit.agent.tool.call`).
   - **`body`** — the log record's body, serialized as JSONB. This is the full structured JSON audit event.
3. The exporter writes extracted fields into the `templogs.logs` table.

### Batch Insert

4. The exporter uses batch inserts for efficiency. Multiple log records in a single export call are written in a single `INSERT` statement with multiple value tuples.
5. Batch size is bounded by what the Collector pipeline delivers per export call (controlled by the Collector's `batch` processor if configured, or by the receiver's internal batching).
6. Each batch insert is wrapped in a single database transaction. If any row fails, the entire batch is rolled back and the export call returns an error (the Collector retries per its retry policy).

### Connection Management

7. The exporter connects to PostgreSQL using a DSN provided via the `dsn` configuration field. The DSN is injected by the lightspeed-operator via environment variable.
8. The exporter maintains a connection pool. Pool size is not user-configurable — it uses sensible defaults for a single-replica Collector.
9. TLS is always enabled. The exporter uses the service-ca certificate bundle mounted by the lightspeed-operator.
10. On connection failure, the exporter returns an error to the Collector pipeline. The Collector's built-in retry mechanism handles retries with exponential backoff.

### Error Handling

11. The exporter does not drop log records silently. If a write fails, the export call returns an error.
12. The Collector's retry and queue mechanisms handle transient failures (Postgres restarts, network blips).
13. If the `templogs.logs` table does not exist (schema not bootstrapped), inserts fail with a Postgres error. The exporter surfaces this as an export error — it does not create tables.

### Configuration

14. The exporter accepts the following configuration fields:

| Field | Type | Required | Description |
|---|---|---|---|
| `dsn` | string | yes | PostgreSQL connection string. Supports env var expansion (`${POSTGRES_DSN}`). |
| `schema` | string | yes | PostgreSQL schema name (e.g., `templogs`). |
| `table` | string | yes | Table name within the schema (e.g., `logs`). |

15. The exporter validates its configuration at startup. Missing or empty required fields cause the Collector to fail to start (fail-fast).

## Implementation

### Go Package Structure

```
postgresexporter/
├── config.go       # Configuration struct and validation
├── exporter.go     # Exporter factory and ConsumeLogs implementation
├── factory.go      # OTel component factory registration
└── writer.go       # PostgreSQL batch insert logic
```

### OTel Component Interface

16. The exporter implements the `exporter.Logs` interface from the OTel Collector SDK.
17. The factory function registers the exporter under the type name `postgres`.
18. The exporter's `ConsumeLogs` method receives `plog.Logs` and writes them to PostgreSQL.

### SQL

19. The insert statement uses parameterized queries to prevent SQL injection:
    ```sql
    INSERT INTO {schema}.{table} (trace_id, timestamp, event, body)
    VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ...
    ```
20. The schema and table names are validated at configuration time (alphanumeric + underscore only) and are not parameterizable — they are compiled into the SQL statement.

### Dependencies

21. The exporter uses `pgx` (not `database/sql` + `pq`) for PostgreSQL connectivity. `pgx` provides native support for batch operations and connection pooling.
22. No ORM or migration framework. The exporter writes to an existing table — schema creation is the lightspeed-operator's responsibility.

## Cross-References

- Parent spec: `ols/.ai/spec/what/templog.md` — Schema DDL, configuration surface
- `what/collector.md` — OCB build, Collector configuration
