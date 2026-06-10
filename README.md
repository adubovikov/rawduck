# 🦆 RawDuck

**Schema-less JSON analytics for DuckDB, RawMergeTree style.**

RawDuck brings the [RawTree](https://rawtree.com/blog/introducing-rawtree) *"ingest first, schema later"* model to DuckDB:
throw raw JSON at a table that doesn't exist yet, and RawDuck creates it, types it, flattens it, and
evolves it as the data changes shape — no `CREATE TABLE`, no schema declarations, no `json_extract`
spaghetti at query time.

Where most engines keep schema-less data as opaque JSON strings (and pay for it at every query),
RawDuck **shreds eagerly at ingest**: nested objects become real typed columns with dotted names,
so queries run at native columnar speed.

```sql
LOAD rawduck;

-- no table 'events' exists yet
SELECT * FROM raw_ingest('events', '[
    {"id": 1, "action": "click", "ts": "2024-01-15T10:30:00", "user": {"name": "alice", "plan": "pro"}},
    {"id": 2, "action": "view",  "ts": "2024-01-15T10:31:00", "user": {"name": "bob"}}
]');
-- ┌─────────┬─────────┬───────────────┬─────────────────┬──────┐
-- │  table  │ created │ columns_added │ columns_widened │ rows │
-- │ events  │ true    │             5 │               0 │    2 │
-- └─────────┴─────────┴───────────────┴─────────────────┴──────┘

DESCRIBE events;
-- id BIGINT, action VARCHAR, ts TIMESTAMP, user.name VARCHAR, user.plan VARCHAR

SELECT "user.name", count(*) FROM events GROUP BY 1;   -- real columns, real speed
```

Ingest again with a different shape and the table follows the data:

```sql
SELECT * FROM raw_ingest('events', '[{"id": 3.5, "action": "buy", "amount": 99.5}]');
-- amount added as DOUBLE, id widened BIGINT -> DOUBLE, missing keys read as NULL
```

## Benchmark: typed columns vs a JSON column

One hour of real [GH Archive](https://www.gharchive.org/) data — **247,199 GitHub events, 956 MB
of NDJSON, wildly heterogeneous payloads** (the same data RawBench uses). RawDuck ingested it with
one `raw_ingest_file` call, automatically creating **914 typed columns**. The baseline is the
standard DuckDB JSON extension pattern: a single `JSON` column queried with `->>` path extraction.

Identical queries, same machine (Apple Silicon, DuckDB v1.5.3), best of 3:

| RawBench-style query | JSON column (`->>`) | RawDuck typed columns | speedup |
|---|---:|---:|---:|
| count by event type | 231 ms | 1 ms | **231×** |
| top repos by pushes | 268 ms | 3 ms | **89×** |
| distinct repos per actor | 457 ms | 10 ms | **46×** |
| sum of push payload sizes | 265 ms | 1 ms | **265×** |
| events per minute | 236 ms | 3 ms | **79×** |
| *all five combined* | *1.46 s* | *18 ms* | **~80×** |

Storage is smaller too, despite 914 columns: **627 MB** (typed + compressed) vs **1.05 GB** for the
JSON-column table — the raw input was 956 MB.

The trade-off is ingest: shredding into 914 compressed columns costs ~190 s for the full hour
(~1,300 events/s) vs ~1.4 s to load opaque JSON strings. Profiling shows the time goes into
DuckDB's columnar storage itself (FSST/dictionary compression, WAL) — it's the one-time price of
making every later query 45–265× faster.

Example queries:

```sql
SELECT * FROM raw_ingest_file('gh_events', '/data/2024-01-15-10.json');

SELECT type, count(*) FROM gh_events GROUP BY type ORDER BY 2 DESC;          -- 1 ms
SELECT "repo.name", count(*) AS pushes FROM gh_events
WHERE type = 'PushEvent' GROUP BY 1 ORDER BY pushes DESC LIMIT 10;           -- 3 ms
```

## Functions

| Function | Kind | Description |
|---|---|---|
| `raw_ingest(table, payload)` | table | Schema-less ingest: auto-creates the table, adds new columns, widens conflicting types, inserts. Accepts a JSON array, a single object, scalars, or NDJSON. Returns `(table, created, columns_added, columns_widened, rows)`. |
| `raw_ingest_file(table, path, batch_size := 30000)` | table | Streaming ingest of NDJSON files (gzip auto-detected, any DuckDB filesystem). Reads in batches, evolving the schema between batches, with bounded memory. |
| `raw_records(payload)` | table | Parse + infer + flatten a JSON payload into typed rows without touching any table. The pure-function core of `raw_ingest`, with vectorized row-major extraction. |
| `raw_stats()` | table | Observed predicate statistics: which columns queries actually filter on, per table. Collected automatically by an optimizer hook from pushed-down filters. |
| `raw_optimize(table)` | table | RawMergeTree-style adaptive layout: physically reorders the table by its most-filtered columns (from `raw_stats`), improving zone-map pruning. Returns the chosen `ORDER BY`. |
| `raw_type(json)` | scalar | Concrete type of a JSON value (RawTree's `dynamicType()`): `Null`, `Bool`, `Int64`, `UInt64`, `Double`, `String`, `Array`, `Object`. |
| `raw_infer(json)` | scalar | The DuckDB type RawDuck inference assigns to a value, e.g. `BIGINT`, `TIMESTAMP`, `DOUBLE[]`, or the full flattened layout for objects: `OBJECT(a BIGINT, b.c VARCHAR)`. |

## Adaptive layout from observed predicates

Like RawMergeTree's adaptive primary keys, RawDuck watches how tables are actually queried.
An optimizer hook records every filter that DuckDB pushes into a table scan; `raw_optimize`
turns those observations into a physical sort order:

```sql
SELECT count(*) FROM gh_events WHERE type = 'PushEvent';
SELECT count(*) FROM gh_events WHERE type = 'WatchEvent';
SELECT sum("payload.size") FROM gh_events WHERE type = 'PushEvent' AND "repo.id" > 700000000;

SELECT * FROM raw_stats();
-- ┌─────────────────────┬─────────┬──────────────┐
-- │        table        │ column  │ filter_count │
-- │ memory.main.gh_events │ repo.id │            1 │
-- │ memory.main.gh_events │ type    │            3 │
-- └─────────────────────┴─────────┴──────────────┘

SELECT * FROM raw_optimize('gh_events');
-- ┌───────────┬───────────────────┬────────┐
-- │   table   │     order_by      │  rows  │
-- │ gh_events │ "type", "repo.id" │ 247199 │
-- └───────────┴───────────────────┴────────┘
```

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

## DuckLake as a backend

`raw_ingest` speaks plain catalog SQL (`CREATE TABLE` / `ALTER TABLE ADD COLUMN` /
`ALTER COLUMN SET DATA TYPE` / `INSERT`), so it works against any attached catalog —
including [DuckLake](https://ducklake.select), which gets you schema-less ingestion straight
into a lakehouse with snapshots and schema evolution tracked in the metadata:

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

The sqllogictests in `test/sql/` cover all standard JSON types (null, boolean, integer, double,
string, array, object), nested flattening, NDJSON, type widening, schema evolution, structural
conflicts, streaming file ingestion, predicate statistics + adaptive reordering, and ingestion
into DuckLake catalogs (`test/sql/ducklake.test`, runs when the `ducklake` extension is available).

## Roadmap

- parallel payload parsing and multi-threaded ingestion
- auto-projections for repeated low-cardinality aggregations
- incremental `raw_optimize` (reorder only new row groups, RawMergeTree merge-style)
- cardinality-aware `ORDER BY` selection (prefer low-cardinality leading columns)

---

Based on the [DuckDB extension template](https://github.com/duckdb/extension-template).
JSON parsing via DuckDB's vendored [yyjson](https://github.com/ibireme/yyjson).
