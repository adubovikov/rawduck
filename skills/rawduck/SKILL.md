---
name: rawduck
description: >-
  Use when working with RawDuck in DuckDB: schema-less JSON/OTLP ingest, ATTACH stores,
  raw_ingest/raw_ingest_file, the in-process HTTP API (raw_serve), OTLP routes, transforms,
  schema evolution, raw_optimize/raw_project, MCP/SDK clients, and error handling.
metadata:
  author: rawduck
  version: "0.1.0"
---

# RawDuck

## RawDuck — Agent-Friendly Schema-Less Analytics in DuckDB

RawDuck is a DuckDB extension: ingest JSON, NDJSON, or OTLP without declaring a schema first.
Payloads are shredded into native typed columns at ingest; the schema evolves as new fields appear.
Query at columnar speed — no `json_extract` on every scan.

**Docs in repo:** [README.md](../../README.md) · [BENCHMARK.md](../../BENCHMARK.md) · [docs/SDK.md](../../docs/SDK.md) · [docs/MCP.md](../../docs/MCP.md)

## Quick Start (SQL)

```sql
LOAD rawduck;

-- Local RawMergeTree store (persists like any DuckDB database)
ATTACH 'rawduck:store.db' AS raw;

-- Schema-less ingest: table + columns emerge from the data
INSERT INTO raw.ingest.events VALUES
  ('{"action":"click","user":{"name":"alice","plan":"pro"}}');

DESCRIBE raw.events;
-- action VARCHAR, user.name VARCHAR, user.plan VARCHAR

SELECT "user.name", count(*) FROM raw.events GROUP BY 1;
```

One-shot ingest without ATTACH:

```sql
CALL raw_ingest('events', '[{"action":"click"},{"action":"view","region":"eu"}]');
-- returns (table, created, columns_added, columns_widened, rows, errors)

CALL raw_ingest_file('events', 'events.ndjson.gz');   -- streaming file ingest
```

## HTTP API (SDK / MCP / curl)

Start the in-process server (keep the DuckDB session open):

```sql
LOAD rawduck;
SELECT * FROM raw_serve(host := '127.0.0.1', port := 9999, token := 'rt_secret');
-- async := false by default — rows visible when insert returns
SELECT * FROM raw_serve_stop();
```

Base URL: `http://127.0.0.1:9999` (localhost by default).

### Authentication

Bearer token on every request except `GET /health`:

```text
Authorization: Bearer rt_secret
```

The `token` argument to `raw_serve` / `raw_serve_grpc` must match. There are no project-scoped
API keys or org management on a local database — `project` / `organization` in list/describe
responses are always `{ "name": "default" }`.

### curl Quick Start

```bash
BASE="http://127.0.0.1:9999"
TOKEN="rt_secret"

# Health
curl -sf "$BASE/health"

# Insert (table auto-created)
curl -sf -X POST "$BASE/v1/tables/events" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '[{"action":"click","user":"alice"}]'
# {"inserted":1}

# Query — rows are objects in "data", plus meta/statistics/hints
curl -sf -X POST "$BASE/v1/query" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"sql":"SELECT action, count(*) AS c FROM events GROUP BY action"}'

# List / describe
curl -sf "$BASE/v1/tables" -H "Authorization: Bearer $TOKEN"
curl -sf "$BASE/v1/tables/events" -H "Authorization: Bearer $TOKEN"

# Drop
curl -sf -X DELETE "$BASE/v1/tables/events" -H "Authorization: Bearer $TOKEN"
```

Gzip request bodies (`Content-Encoding: gzip`) and gzip responses (`Accept-Encoding: gzip`) are
supported.

### TypeScript SDK

Point [`@rawtree/sdk`](https://www.npmjs.com/package/@rawtree/sdk) at the local server — insert,
query, and table metadata match the HTTP API shape:

```ts
import { RawTree } from "@rawtree/sdk";
const client = new RawTree({ apiKey: "rt_secret", baseUrl: "http://127.0.0.1:9999" });
await client.insert("events", [{ action: "click" }]);
await client.query("SELECT action FROM events");
```

See [docs/SDK.md](../../docs/SDK.md). For OpenTelemetry, [`@rawtree/otel`](https://www.npmjs.com/package/@rawtree/otel) posts OTLP JSON via `insert()`; RawDuck auto-detects the envelope.

### MCP

Configure [`@rawtree/mcp`](https://github.com/rawtreedb/rawtree-mcp) with `--api-url` and
`--api-key`. Works: `check-health`, `insert-json`, `run-query`, `list-tables`, `describe-table`,
`delete-table`, transforms on insert. Not available locally: projects, API keys, logs, URL ingest.

See [docs/MCP.md](../../docs/MCP.md).

## API Reference (Agent-Oriented)

| Method | Path | Body / notes |
|---|---|---|
| `GET` | `/health` | `{"status":"ok"}` — no auth |
| `POST` | `/v1/query` | `{"sql":"..."}` → `{meta, data, rows, statistics, hints}` |
| `GET` | `/v1/tables` | `{tables:[...], project, organization}` |
| `GET` | `/v1/tables/{table}` | `{table:{name, columns, total_rows, total_bytes, created_at, ...}}` |
| `POST` | `/v1/tables/{table}` | JSON object, array, or NDJSON body → `{"inserted":N}` |
| `DELETE` | `/v1/tables/{table}` | drop table |
| `POST` | `/otlp/v1/{traces,logs,metrics}` | OTLP/HTTP JSON or protobuf; spec-shaped `partialSuccess` |

**Insert query params:** `?transform=<name>`, `?explode=<dotted.path>`, `?ignore_errors=true`

**Insert body modes:** single object `{...}`, array `[{...}]`, or NDJSON lines.

**Not on local RawDuck:** `/v1/projects`, `/v1/keys`, `/v1/logs`, URL ingest (`?url=`).

### Hosted API → RawDuck mapping

| Hosted workflow | RawDuck equivalent |
|---|---|
| `rtree insert --table t --data '...'` | `POST /v1/tables/t` or `CALL raw_ingest('t', '...')` |
| `rtree insert --file ./x.jsonl` | `CALL raw_ingest_file('t', './x.jsonl')` |
| `rtree insert --transform otlp-traces` | `?transform=otlp-traces` or `transform := 'otlp-traces'` |
| `rtree query --sql "..."` | `POST /v1/query` or run SQL in DuckDB |
| `rtree table list/describe` | `GET /v1/tables` / `GET /v1/tables/{t}` or `DESCRIBE` |
| `rtree login` / API keys | set `token` on `raw_serve()`; use as Bearer token |
| `rtree logs` | not available — use DuckDB / application logs |

## Ingest Transforms

Built-ins (envelope → one row per nested record, wrapper fields merged):

```text
otlp-traces, otlp-logs, otlp-metrics
cloudwatch-logs, cloudtrail, firehose
```

```sql
-- SQL
CALL raw_ingest('traces', payload, transform := 'otlp-traces');
CALL raw_ingest_file('logs', 'export.ndjson', transform := 'otlp-logs');

-- HTTP (explicit or auto-detected for OTLP JSON envelopes)
POST /v1/tables/traces?transform=otlp-traces
POST /v1/tables/spans          -- auto-detects resourceSpans → otlp-traces

-- INSERT lane via session setting
SET rawduck_insert_transform = 'otlp-traces';
INSERT INTO raw.ingest.spans SELECT json FROM read_json('traces.ndjson', ...);
RESET rawduck_insert_transform;

-- Custom transforms (compose with files/tables)
SELECT raw_transform_define('my-batch', 'data.items');
CALL raw_transforms();
```

Generic nested explode (any shape):

```sql
CALL raw_ingest('events', payload, explode := 'batch.items');
```

### OpenTelemetry collectors

OTLP/HTTP (JSON or protobuf):

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:9999/otlp
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer rt_secret"
```

Default tables: `otel_traces`, `otel_logs`, `otel_metrics`. Override with headers
`x-rawduck-traces-table`, `x-rawduck-logs-table`, `x-rawduck-metrics-table`, or
`x-rawduck-table`.

OTLP/gRPC (opt-in build: `make release RAWDUCK_ENABLE_GRPC=1`):

```sql
CALL raw_serve_grpc(port := 4317, token := 'rt_secret');
```

OTLP shredding: nested envelopes exploded, KeyValue attributes → flat typed columns, trace/span
ids as hex, enums as integers, `*UnixNano` as `BIGINT`.

## Schema Evolution (Typed Columns)

Unlike per-row dynamic typing, RawDuck assigns one **column type per path** and widens monotonically:

- new keys → `ALTER TABLE ADD COLUMN`
- scalar conflicts → widen toward `VARCHAR` (or `DOUBLE` for numeric conflicts)
- structural conflicts (object vs scalar, mixed arrays) → `JSON`
- missing keys in a row → `NULL` (not column defaults)

Query flattened paths directly:

```sql
SELECT "user.name", "http.status_code" FROM traces WHERE "http.status_code" >= 500;
```

Inspect inference without writing:

```sql
SELECT * FROM raw_records('{"a":1,"b":{"c":"x"}}');
SELECT raw_type('{"x":1}');    -- Object
SELECT raw_infer('{"x":1}');   -- OBJECT(x BIGINT)
```

Dirty streams: `ignore_errors := true` — bad lines counted in `errors`, good rows still land.

## Query Tips

- **HTTP `/v1/query` accepts any SQL DuckDB allows** — not SELECT-only. Agents can run DDL/DML when appropriate; prefer read queries for inspection workflows.
- Use bare column names in filters and GROUP BY so `raw_optimize` / `raw_project` can help:
  `WHERE "http.status_code" = 500`, not `WHERE CAST(... AS VARCHAR) = '500'`.
- Standard DuckDB SQL: `count(*)`, `GROUP BY`, window functions, joins across RawDuck and regular tables.
- Parameterize in application code, then send the final SQL string in `{"sql":"..."}`.

```sql
-- Preview parsed rows without touching a table
SELECT * FROM raw_records(payload, transform := 'otlp-traces') LIMIT 5;

-- After workload develops
CALL raw_stats();
CALL raw_optimize('events');              -- physical sort by hottest filter columns
CALL raw_project('events');               -- materialize hottest GROUP BY
SET rawduck_use_projections = true;       -- transparent count(*) rewrite (off by default)
```

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `rawduck_async_insert` | `false` | Buffer `raw_ingest` / file ingest; flush in background |
| `rawduck_async_max_data_size` | `1048576` | Async buffer byte threshold |
| `rawduck_async_busy_timeout_ms` | `200` | Async buffer age threshold |
| `rawduck_insert_transform` | `''` | Default transform for `INSERT INTO …ingest.*` |
| `rawduck_use_projections` | `false` | Rewrite eligible aggregations onto projections |
| `rawduck_overlap_flush` | `false` | Flush completed row groups mid large stable import (~10–15% faster, higher peak memory) |

Async mode: call `CALL raw_flush()` before shutdown or before queries that must see buffered rows.
HTTP/gRPC default to synchronous ingest; pass `async := true` on `raw_serve` only for
fire-and-forget producers.

## ATTACH Stores

```sql
ATTACH 'rawduck:store.db' AS raw;                    -- local on-disk store
ATTACH 'rawduck:quack:host:port' AS raw (TOKEN '…'); -- remote over Quack (server needs RawDuck)
```

- `raw.ingest.*` — schema-less JSON lane (creation + evolution)
- `raw.<table>` — normal typed DuckDB tables after ingest
- DuckLake backend: `ATTACH 'ducklake:…' AS lake; CALL raw_ingest('lake.main.events', payload);`

## Supported Types

RawDuck maps JSON to DuckDB types: `BOOLEAN`, `BIGINT`, `DOUBLE`, `VARCHAR`, `DATE`, `TIMESTAMP`,
`JSON`, typed `LIST`s (`BIGINT[]`, …). Use normal DuckDB casts in queries:

```sql
SELECT CAST("http.status_code" AS INTEGER), ts::TIMESTAMP FROM events;
```

## Errors

HTTP errors: `{"error":"<message>","hint":"<optional>"}` with appropriate status (401, 400, 404, 503).

Common agent pitfalls:

- **Insert then immediate query fails** — if `async := true` on `raw_serve` or
  `SET rawduck_async_insert = true`, rows are not visible until flush. Use sync defaults for
  insert-then-query workflows (MCP, SDK smoke tests).
- **Table does not exist after HTTP insert** — server not running, wrong port/token, or async mode.
- **OTLP columns look wrong** — missing transform on non-auto-detected payloads; use
  `?transform=otlp-traces` or dedicated `/otlp/v1/traces` route.
- **Schema churn slow on huge heterogeneous files** — expected for wide evolving schemas; OTEL
  workloads with stable shapes ingest at ~600k+ records/s (see BENCHMARK.md).

## Key Functions

| Function | Purpose |
|---|---|
| `raw_ingest(table, payload)` | Ingest JSON/NDJSON in caller's transaction |
| `raw_ingest_file(table, path)` | Streaming file ingest (gzip ok; byte-aware batching for fat lines) |
| `raw_records(payload)` | Parse/infer/flatten without writing |
| `raw_serve` / `raw_serve_stop` | HTTP API lifecycle |
| `raw_serve_grpc` / `raw_serve_grpc_stop` | OTLP gRPC collector (opt-in build) |
| `raw_flush()` | Drain async-insert buffers |
| `raw_stats` / `raw_optimize` / `raw_project` | Workload-driven layout and projections |
| `raw_transform_define` / `raw_transforms` | Register/list ingest transforms |

All ingest functions accept `transform`, `explode`, `ignore_errors`. `raw_ingest_file` also
accepts `batch_size` (default 30000 lines; byte cap applies automatically for container payloads).
