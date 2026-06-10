# 🦆 RawDuck

**Schema-less JSON analytics for DuckDB, RawMergeTree style.**

RawDuck brings the [RawTree](https://rawtree.com/blog/introducing-rawtree) *"ingest first, schema later"* model to DuckDB:
throw raw JSON at a table that doesn't exist yet, and RawDuck creates it, types it, flattens it, and
evolves it as the data changes shape — no `CREATE TABLE`, no schema declarations, no `json_extract`
spaghetti at query time.

Where most engines keep schema-less data as opaque JSON strings (and pay for it at every query),
RawDuck **shreds eagerly at ingest**: nested objects become real typed columns with dotted names,
so queries run at native columnar speed. Ingestion runs through DuckDB's catalog and storage APIs
directly, inside your transaction — `BEGIN` / `ROLLBACK` work exactly as you'd expect.

```sql
ATTACH 'rawduck:store.db' AS raw;

-- no table 'events' exists yet
SELECT * FROM raw_ingest('raw.events', '[
    {"id": 1, "action": "click", "ts": "2024-01-15T10:30:00", "user": {"name": "alice", "plan": "pro"}},
    {"id": 2, "action": "view",  "ts": "2024-01-15T10:31:00", "user": {"name": "bob"}}
]');

DESCRIBE raw.events;
-- id BIGINT, action VARCHAR, ts TIMESTAMP, user.name VARCHAR, user.plan VARCHAR
```

RawMergeTree tables are regular DuckDB tables — every query, statement, and tool works
transparently at native speed:

```sql
SELECT "user.name", count(*) FROM raw.events GROUP BY 1;
INSERT INTO raw.events VALUES (3, 'buy', now(), 'carol', 'pro');
UPDATE raw.events SET "user.plan" = 'enterprise' WHERE id = 1;
CREATE TABLE raw.daily AS SELECT date_trunc('day', ts) AS day, count(*) FROM raw.events GROUP BY 1;
```

Ingest again with a different shape and the table follows the data: new keys become columns,
conflicting types widen, missing keys read as `NULL` — nothing is ever dropped.

## Benchmark: one hour of GitHub, three ways

Real [GH Archive](https://www.gharchive.org/) data — **247,199 GitHub events, 956 MB of NDJSON,
wildly heterogeneous payloads** (the dataset RawBench uses). One `raw_ingest_file` call shredded it
into **914 typed columns**, schema evolution included. The baseline is the standard DuckDB JSON
extension pattern: a single `JSON` column queried with `->>` paths.

Same machine (Apple Silicon, DuckDB v1.5.3), best of 3:

| RawBench-style query | JSON column (`->>`) | RawDuck typed columns | speedup |
|---|---:|---:|---:|
| count by event type | 231 ms | 1 ms | **231×** |
| top repos by pushes | 268 ms | 3 ms | **89×** |
| distinct repos per actor | 457 ms | 10 ms | **46×** |
| sum of push payload sizes | 265 ms | 1 ms | **265×** |
| events per minute | 236 ms | 3 ms | **79×** |
| *all five combined* | *1.46 s* | *18 ms* | **~80×** |

| | JSON column | RawDuck |
|---|---:|---:|
| ingest (full hour, 956 MB) | 1.4 s | **11.3 s** |
| storage on disk | 1.05 GB | **627 MB** |

Ingest runs at ~22,000 events/s (~85 MB/s) — a one-time cost within an order of magnitude of
loading opaque JSON strings, in exchange for every later query being 45–265× faster and the data
40% smaller on disk.

```sql
SELECT * FROM raw_ingest_file('gh_events', '/data/2024-01-15-10.json.gz');   -- 11.3s, 914 columns

SELECT type, count(*) FROM gh_events GROUP BY type ORDER BY 2 DESC;          -- 1 ms
SELECT "repo.name", count(*) AS pushes FROM gh_events
WHERE type = 'PushEvent' GROUP BY 1 ORDER BY pushes DESC LIMIT 10;           -- 3 ms
```

## Functions

| Function | Kind | Description |
|---|---|---|
| `raw_ingest(table, payload)` | table | Schema-less ingest: auto-creates the table, adds new columns, widens conflicting types, appends — natively, inside your transaction. Accepts a JSON array, a single object, scalars, or NDJSON. Returns `(table, created, columns_added, columns_widened, rows, errors)`. |
| `raw_ingest_file(table, path, batch_size := 30000)` | table | Streaming ingest of NDJSON files (gzip auto-detected, any DuckDB filesystem) in bounded-memory batches, evolving the schema between batches. The whole file is one atomic operation. |
| `raw_records(payload)` | table | Parse + infer + flatten a JSON payload into typed rows without touching any table. |
| `raw_stats()` | table | Observed predicate statistics: which columns queries actually filter on, collected automatically from pushed-down filters. |
| `raw_optimize(table)` | table | RawMergeTree-style adaptive layout: physically reorders the table by its most-filtered columns (from `raw_stats`). |
| `raw_type(json)` | scalar | Concrete type of a JSON value (RawTree's `dynamicType()`): `Null`, `Bool`, `Int64`, `UInt64`, `Double`, `String`, `Array`, `Object`. |
| `raw_infer(json)` | scalar | The DuckDB type RawDuck assigns to a value, e.g. `BIGINT`, `DOUBLE[]`, or the flattened layout for objects: `OBJECT(a BIGINT, b.c VARCHAR)`. |

All ingest functions accept `transform := '...'`, `explode := '...'` and `ignore_errors := true`.

## ATTACH: RawMergeTree stores

```sql
ATTACH 'rawduck:store.db' AS raw;
```

A RawDuck store is a native DuckDB database under a RawDuck-typed catalog: everything DuckDB can
do — joins, window functions, updates, exports, other extensions — works on RawMergeTree tables
transparently and at full native speed, while the store identifies itself for RawDuck's ingestion
and adaptive-layout machinery. Stores persist and reattach like any database file.

Note on transparency limits: DuckDB's binder resolves `INSERT` column lists against the current
schema, so a plain `INSERT` cannot create tables or add columns (no catalog can change that —
binding happens before any catalog hook). Schema-less creation and evolution flow through the
ingest functions, which run in the same transaction as the rest of your statements.

## Transforms

Like RawTree, RawDuck reshapes envelope-style telemetry at ingest time: one row per nested event,
with the wrapper's fields merged into each row.

```sql
-- {"owner":"123","logGroup":"/aws/lambda/api","logEvents":[{"id":"1","message":"started"},...]}
SELECT * FROM raw_ingest('logs', payload, transform := 'cloudwatch-logs');
-- one row per log event, with owner and logGroup columns on each

-- the generic form works for any envelope shape
SELECT * FROM raw_ingest('events', payload, explode := 'batch.items');
```

Built-in transform names: `cloudwatch-logs`, `cloudtrail`, `firehose`. Dirty NDJSON streams can be
ingested with `ignore_errors := true`; skipped lines are counted in the `errors` column.

## The type lattice

Per JSON path, RawDuck infers the narrowest type that holds everything seen, widening monotonically
as data arrives (existing columns are `ALTER`ed in place, never rewritten destructively):

```
            BOOLEAN   BIGINT ──> DOUBLE     DATE ──> TIMESTAMP
                │        │          │          │          │
                └────────┴────> VARCHAR <──────┴──────────┘      (scalar conflicts)

            object vs scalar, mixed arrays, arrays of objects ──> JSON   (structural conflicts)
```

- integers out of `BIGINT` range degrade to `DOUBLE`
- ISO `DATE` / `TIMESTAMP` strings are sniffed into temporal columns
- homogeneous scalar arrays become typed `LIST`s (`BIGINT[]`, nested `BIGINT[][]`, …)
- nothing is ever dropped: structurally conflicting values are preserved verbatim as `JSON`

## Adaptive layout from observed predicates

An optimizer hook records every filter DuckDB pushes into a table scan; `raw_optimize` turns those
observations into a physical sort order, RawMergeTree adaptive-primary-key style:

```sql
SELECT count(*) FROM gh_events WHERE type = 'PushEvent';
SELECT sum("payload.size") FROM gh_events WHERE type = 'PushEvent' AND "repo.id" > 700000000;

SELECT * FROM raw_stats();
-- gh_events | type    | 2
-- gh_events | repo.id | 1

SELECT * FROM raw_optimize('gh_events');
-- gh_events | "type", "repo.id" | 247199
```

## DuckLake as a backend

For non-native catalogs RawDuck falls back to catalog-level SQL, so schema-less ingestion also
works against [DuckLake](https://ducklake.select) — straight into a lakehouse with snapshots and
schema evolution tracked in the metadata:

```sql
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 's3://bucket/raw');
SELECT * FROM raw_ingest('lake.main.events', payload);
```

Catalogs that cannot rewrite columns with expressions (DuckLake rejects `ALTER ... USING`)
degrade gracefully: RawDuck keeps the existing column type and converts incoming values instead.

## Building

```sh
git submodule update --init
GEN=ninja make release
```

Artifacts:

```sh
./build/release/duckdb                                          # shell with rawduck linked in
./build/release/test/unittest                                   # test runner
./build/release/extension/rawduck/rawduck.duckdb_extension      # loadable extension
```

## Tests

```sh
make test
```

The sqllogictests in `test/sql/` cover all standard JSON types, nested flattening, NDJSON, type
widening, schema evolution, structural conflicts, streaming file ingestion, transforms,
error-tolerant ingestion, RawDuck stores (`ATTACH 'rawduck:...'`), transactional rollback,
predicate statistics + adaptive reordering, and DuckLake catalogs (`test/sql/ducklake.test`).

## Roadmap

- parallel payload parsing and multi-threaded appends
- OTLP transforms (`otlp-traces`, `otlp-logs`, `otlp-metrics`)
- auto-projections for repeated low-cardinality aggregations
- incremental `raw_optimize` (reorder only new row groups, RawMergeTree merge-style)
- persisted predicate statistics inside RawDuck stores

---

Based on the [DuckDB extension template](https://github.com/duckdb/extension-template).
JSON parsing via DuckDB's vendored [yyjson](https://github.com/ibireme/yyjson).
