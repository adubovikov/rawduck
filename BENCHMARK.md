# RawDuck Benchmark

Reproduces the numbers in the [README](README.md): RawDuck's shredded typed columns vs the
standard DuckDB JSON extension pattern (a single `JSON` column queried with `->>` paths), on one
hour of real [GH Archive](https://www.gharchive.org/) data — the same dataset family RawBench uses.

Published results (Apple Silicon, 10 cores, DuckDB v1.5.3, 247,199 events / 956 MB NDJSON):

| | JSON column | RawDuck | |
|---|---:|---:|---|
| count by event type | 231 ms | 1 ms | 231× |
| top repos by pushes | 268 ms | 3 ms | 89× |
| distinct repos per actor | 457 ms | 10 ms | 46× |
| sum of push payload sizes | 265 ms | 1 ms | 265× |
| events per minute | 236 ms | 3 ms | 79× |
| ingest | 1.4 s | ~10–11 s | one-time cost |
| storage | 1.05 GB | 636 MB | 40% smaller |

## 1. Build

```sh
git submodule update --init
GEN=ninja make release
alias bduck=./build/release/duckdb
```

## 2. Get the data

Any GH Archive hour works; the published numbers use:

```sh
curl -sL https://data.gharchive.org/2024-01-15-10.json.gz -o gh.json.gz
gunzip -k gh.json.gz       # optional: raw_ingest_file reads the .gz directly
```

## 3. RawDuck ingest

One call shreds the whole hour into typed columns (914 for this file), schema evolution included:

```sql
-- bduck rawduck.db
.timer on
SELECT * FROM raw_ingest_file('gh_events', 'gh.json');
CHECKPOINT;
```

## 4. Baseline ingest

The JSON-column baseline must disable structure inference (`records='false'` alone still infers a
STRUCT and fails on this dataset; the explicit `columns` clause keeps raw JSON):

```sql
-- bduck baseline.db
.timer on
CREATE TABLE gh_raw AS SELECT json
FROM read_json('gh.json', format='newline_delimited', records='false', columns={json: 'JSON'});
CHECKPOINT;
```

## 5. Queries

Run each three times with `.timer on` (against `-readonly` databases); report the best.

RawDuck:

```sql
SELECT type, count(*) AS n FROM gh_events GROUP BY type ORDER BY n DESC;
SELECT "repo.name", count(*) AS n FROM gh_events WHERE type='PushEvent' GROUP BY 1 ORDER BY n DESC LIMIT 10;
SELECT "actor.login", count(DISTINCT "repo.name") AS r FROM gh_events GROUP BY 1 ORDER BY r DESC LIMIT 10;
SELECT sum("payload.size") FROM gh_events WHERE type='PushEvent';
SELECT date_trunc('minute', created_at) AS m, count(*) FROM gh_events GROUP BY m ORDER BY m;
```

Baseline (same queries through JSON path extraction):

```sql
SELECT json->>'$.type' AS type, count(*) AS n FROM gh_raw GROUP BY type ORDER BY n DESC;
SELECT json->>'$.repo.name' AS repo, count(*) AS n FROM gh_raw WHERE json->>'$.type'='PushEvent' GROUP BY 1 ORDER BY n DESC LIMIT 10;
SELECT json->>'$.actor.login' AS a, count(DISTINCT json->>'$.repo.name') AS r FROM gh_raw GROUP BY 1 ORDER BY r DESC LIMIT 10;
SELECT sum(CAST(json->>'$.payload.size' AS BIGINT)) FROM gh_raw WHERE json->>'$.type'='PushEvent';
SELECT date_trunc('minute', CAST(json->>'$.created_at' AS TIMESTAMP)) AS m, count(*) FROM gh_raw GROUP BY m ORDER BY m;
```

## 6. Storage

Compare the database file sizes after `CHECKPOINT`. If a database accumulated DDL rewrites, copy
the table into a fresh file first (`ATTACH 'fresh.db'; CREATE TABLE fresh.t AS FROM t; CHECKPOINT fresh;`).

## 7. Optional: adaptive layout and projections

```sql
-- after the filtered queries above, the optimizer hook has observed the predicates
SELECT * FROM raw_stats();
SELECT * FROM raw_optimize('gh_events');     -- physically reorders by hottest columns
SELECT * FROM raw_project('gh_events');      -- materializes the hottest aggregation
SET rawduck_use_projections = true;          -- transparent rewrite of eligible count(*) queries
```

## Warm-table ingest

The cold numbers above include schema discovery (CREATE + evolution sync points). Re-ingesting
the same hour into the already-evolved table runs the pipeline fully parallel: **~4.9 s**
(~50k events/s, ~196 MB/s) — the steady-state rate once a table's shape has stabilized.

## Realtime small-insert workload

One-shot bulk loads don't show client behavior. To simulate real clients (1–100 events per
insert, mixed shapes), generate a few thousand small `raw_ingest` calls and time the run; on the
reference machine 2,000 inserts (≈44k events) sustain **~2,600 inserts/s (~58k events/s)** with
sub-millisecond average latency and no backpressure. The schema-shape cache makes repeated payload
shapes skip all schema-delta work (the cache invalidates automatically on any ALTER via the
table's storage token).

## Pitfalls

- Don't split NDJSON with Python's `splitlines()` — it splits on ` `/` ` which appear
  raw inside GH Archive strings and corrupts records. Split on `\n` only.
- The shell reports query times with `.timer on`; dot-commands don't work via `duckdb -c`,
  use `-f script.sql`.
- A shallow duckdb submodule clone without tags makes the shell report `v0.0.1`; fetch the
  release tag (`git -C duckdb fetch --depth 1 origin tag v1.5.3`) or extension installs 404.
