#!/usr/bin/env bash
# Write-path benchmark matrix for RawDuck native ingest.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DUCKDB="${DUCKDB:-$ROOT/build/release/duckdb}"
BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR="$BENCH_DIR/data"
OUT="$BENCH_DIR/write_matrix_results.txt"

if [[ ! -x "$DUCKDB" ]]; then
  echo "Build first: cd $ROOT && GEN=ninja make release" >&2
  exit 1
fi

mkdir -p "$DATA_DIR"
python3 "$BENCH_DIR/gen_otlp.py" 100000 "$DATA_DIR" >/dev/null 2>&1 || true

measure_ingest_seconds() {
  local db=$1
  rm -f "$db"
  { time -p "$DUCKDB" "$db" <<SQL >/dev/null
CALL raw_ingest_file('traces', '$DATA_DIR/traces.ndjson', transform := 'otlp-traces');
SQL
  } 2>&1 | awk '/^real/{print $2; exit}'
}

{
  echo "# RawDuck write matrix $(date -Iseconds)"
  echo "duckdb=$DUCKDB"
  echo

  echo "## INSERT ingest narrow (500k rows)"
  rm -f "$BENCH_DIR/insert_bench.db" "$BENCH_DIR/insert_bench.db.wal"
  "$DUCKDB" "$BENCH_DIR/insert_bench.db" <<'SQL'
.timer on
ATTACH 'rawduck:insert_bench.db' AS raw;
INSERT INTO raw.ingest.narrow SELECT '{"a":' || range || '}' FROM range(500000);
SELECT count(*) FROM raw.narrow;
SQL

  echo
  echo "## raw_ingest_file OTLP traces (best of 3 runs, default settings)"
  best=999
  for run in 1 2 3; do
    t=$(measure_ingest_seconds "$BENCH_DIR/file_bench.db")
    echo "  run $run: ${t}s"
    best=$(python3 -c "print(min(float('$t'), float('$best')))")
  done
  rps=$(python3 -c "print(int(round(100000/float('$best'))))")
  echo "  best: ${best}s (~${rps} rows/s)"
  "$DUCKDB" "$BENCH_DIR/file_bench.db" <<SQL
.timer on
CALL raw_ingest_file('traces', '$DATA_DIR/traces.ndjson', transform := 'otlp-traces');
SELECT count(*) FROM traces;
SQL

  echo
  echo "## raw_ingest_file OTLP (overlap_flush=on, pool_min_rows=512)"
  rm -f "$BENCH_DIR/file_overlap.db"
  "$DUCKDB" "$BENCH_DIR/file_overlap.db" <<SQL
.timer on
SET rawduck_overlap_flush = true;
SET rawduck_pool_min_rows = 512;
CALL raw_ingest_file('traces', '$DATA_DIR/traces.ndjson', transform := 'otlp-traces');
SELECT count(*) FROM traces;
SQL

  echo
  echo "## raw_ingest_file OTLP (overlap_flush_auto, pool_min_rows=4096)"
  rm -f "$BENCH_DIR/file_auto.db"
  "$DUCKDB" "$BENCH_DIR/file_auto.db" <<SQL
.timer on
SET rawduck_overlap_flush_auto = true;
SET rawduck_pool_min_rows = 4096;
CALL raw_ingest_file('traces', '$DATA_DIR/traces.ndjson', transform := 'otlp-traces');
SELECT count(*) FROM traces;
SQL

} | tee "$OUT"

echo "Results written to $OUT"
