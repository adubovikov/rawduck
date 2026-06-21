# DuckLake Native Write — Research Spike

**Date:** 2026-06-21  
**Context:** RawDuck write-path improvement plan, Phase 4

## Current behavior

When the target catalog is **not** a native DuckCatalog (`IsDuckCatalog() == false`), [`RawIngestor::IngestFallback`](/home/shurik/Projects/rawduck/src/raw_ingest.cpp) runs:

1. Opens a **second** `Connection` (`fallback_conn`)
2. Runs DDL via SQL (`CREATE TABLE`, `ALTER TABLE ADD COLUMN`, `ALTER ... SET DATA TYPE`)
3. Inserts via `INSERT INTO ... SELECT ... FROM raw_records(payload)` — **re-parsing JSON**

This path is correct but slow (~20× vs native per AGENTS.md invariant #2). DuckLake POC confirmed functional correctness ([`agent_planning/bench/ducklake_poc.sql`](/home/shurik/Projects/rawduck/agent_planning/bench/ducklake_poc.sql), [`test/sql/ducklake.test`](/home/shurik/Projects/rawduck/test/sql/ducklake.test)).

## DuckLake catalog API investigation

DuckLake attaches as a DuckDB extension catalog. Key constraints:

| Constraint | Implication |
|------------|-------------|
| DuckLake is not `IsDuckCatalog()` | RawDuck native `LocalAppend` / `OptimisticDataWriter` path is unavailable |
| `ALTER ... USING expression` unsupported | JSON widening must retry without expression (already handled) |
| Separate transaction from caller | Cannot share caller's `MetaTransaction` for catalog writes |
| Parquet data path external | Commits flush Parquet files + catalog metadata |

**Conclusion:** There is no public DuckDB API today to append to DuckLake tables inside the caller's table-function transaction the way native DuckCatalog allows. The SQL fallback is architecturally required unless DuckLake exposes a C++ append hook.

## Hepic reference architecture

[`hepic-lake-ingest/src/storage/ducklake_writer.hpp`](/home/shurik/Projects/hepic-lake-ingest/src/storage/ducklake_writer.hpp) implements:

- Double-buffer connections (reader slot / flush slot)
- Dedicated flush thread per shard
- `FlushGate` per-table mutex for cross-shard catalog serialization
- Optional WAL before append, checkpoint after flush
- Direct DuckDB `Appender` API (not SQL re-parse)

## Proposed RawDuckLakeWriter (future)

```
┌─────────────────────────────────────────────────────────┐
│ RawDuckLakeWriter (per attached DuckLake catalog)       │
│                                                         │
│  ingest thread(s)  →  parse (raw_json, pure)           │
│         ↓                                               │
│  schema coordinator  →  DDL via dedicated Connection     │
│         ↓                                               │
│  row buffer (typed columns, Arrow or DuckDB DataChunk)  │
│         ↓                                               │
│  flush thread      →  Appender API → DuckLake Parquet │
└─────────────────────────────────────────────────────────┘
```

### Design points

1. **No JSON re-parse on insert** — build `DataChunk` from `RawParsedPayload` same as native `AppendNative`, then use DuckDB Appender on flush connection
2. **Schema evolution** — single-writer DDL queue (like Hepic `FlushGate`), batch ALTERs
3. **Stable schema fast path** — once shape absorbed, skip DDL entirely (mirror `RawSchemaCache`)
4. **Hepic coexistence** — RawDuck dynamic tables (`custom_events`) alongside fixed `hep_proto_*` / `otel_*` in same catalog; no schema collision if table names are distinct

### Integration with Hepic

| Component | Role |
|-----------|------|
| hepic-lake-ingest | Typed HEP/OTLP at line rate (keep) |
| RawDuck + RawDuckLakeWriter | Sidecar schema-less JSON into `lake.main.*` |
| hepic-lake FlightSQL | Read both without changes |

### Effort estimate

| Phase | Work | Risk |
|-------|------|------|
| Spike: Appender-based insert (no SQL raw_records) | 1 week | Medium — verify DuckLake Appender accepts evolved schemas |
| Double-buffer flush thread | 2 weeks | High — catalog commit races |
| Production hardening + tests | 1 week | Medium |

### Recommendation

**Short term:** Use native `rawduck:store.db` for maximum write throughput; use DuckLake fallback for integration/testing only.

**Medium term:** Implement `RawDuckLakeWriter` with Appender-based flush (eliminate `raw_records` re-parse) — largest win on DuckLake path without requiring same-transaction native append.

**Not recommended:** Attempting to force DuckLake through `LocalAppend` in caller transaction — violates DuckLake's transaction model and risks catalog corruption.
