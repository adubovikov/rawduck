#!/usr/bin/env bash
# Multi-round A/B with median / p95 to separate real regression from noise.
#
# Usage:
#   ./scripts/run_vs_release_stats.sh              # 5 rounds, 50k rows
#   ./scripts/run_vs_release_stats.sh 100000       # 50k rows implied: rounds=5
#   ROUNDS=10 ./scripts/run_vs_release_stats.sh 50000
#
# Each round: single cold-db write per build (paired), read avg (best of 3 × 4 queries).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/ab_bench_lib.sh
source "$ROOT/scripts/ab_bench_lib.sh"

ROWS="${1:-50000}"
ROUNDS="${ROUNDS:-5}"
RELEASE_VERSION="${RELEASE_VERSION:-v0.0.2}"
CACHE="$ROOT/agent_planning/bench/.cache/release"
DATA="$ROOT/agent_planning/bench/data/compare_events.ndjson"
OUT_DIR="$ROOT/agent_planning/bench/vs_release"
RESULTS="$OUT_DIR/ab_stats_results.txt"
CSV="$OUT_DIR/ab_stats_rounds.csv"
LOCAL_BIN="$ROOT/build/release/duckdb"

mkdir -p "$CACHE" "$OUT_DIR"

if [[ ! -x "$LOCAL_BIN" ]]; then
    echo "Local build missing. Run: cd $ROOT && GEN=ninja make release" >&2
    exit 1
fi

ab_install_release "$CACHE" "$RELEASE_VERSION"
ab_install_local_wrapper "$CACHE" "$LOCAL_BIN"
python3 "$ROOT/scripts/gen_compare_data.py" "$ROWS" "$DATA" >/dev/null

SOURCE_BYTES=$(ab_file_bytes "$DATA")
SOURCE_MB=$(awk "BEGIN {printf \"%.2f\", $SOURCE_BYTES / 1048576}")

echo "round,build,write_ms,read_avg_ms,disk_bytes,cols" >"$CSV"

{
    echo "# RawDuck A/B stats: local vs ${RELEASE_VERSION}"
    echo "# $(date -Iseconds)"
    echo "# Rounds: $ROUNDS | Rows: $ROWS | NDJSON: ${SOURCE_MB} MB"
    echo "# Method: one cold write per round per build; read = best-of-3 avg over 4 queries"
    echo ""
} | tee "$RESULTS"

round=1
while (( round <= ROUNDS )); do
    echo "== Round $round / $ROUNDS ==" >&2

    rel_db="$OUT_DIR/stats_r${round}_release.db"
    loc_db="$OUT_DIR/stats_r${round}_local.db"

    rel=$(ab_bench_round "release" "$CACHE/bin/rawduck" "$rel_db" "$DATA" "$ROWS" once)
    loc=$(ab_bench_round "local" "$CACHE/bin/rawduck_local" "$loc_db" "$DATA" "$ROWS" once)

    IFS=',' read -r rel_w rel_r rel_d rel_c <<< "$rel"
    IFS=',' read -r loc_w loc_r loc_d loc_c <<< "$loc"

    echo "  release: write=${rel_w}ms read_avg=${rel_r}ms disk=${rel_d}" >&2
    echo "  local:   write=${loc_w}ms read_avg=${loc_r}ms disk=${loc_d}" >&2
    w_delta=$(awk -v a="$loc_w" -v b="$rel_w" 'BEGIN{printf "%+.1f", (a-b)/b*100}')
    echo "  write delta: ${w_delta}% (local vs release)" >&2

    echo "${round},release,${rel}" >>"$CSV"
    echo "${round},local,${loc}" >>"$CSV"
    round=$((round + 1))
done

python3 - "$CSV" "$ROWS" "$RELEASE_VERSION" "$ROUNDS" "$RESULTS" <<'PY'
import csv
import math
import sys
from pathlib import Path
from statistics import median

csv_path, rows, version, rounds, out_path = sys.argv[1:6]
rows = int(rows)
rounds = int(rounds)

by_round = {}
with open(csv_path, newline="") as f:
    for r in csv.DictReader(f):
        rd = int(r["round"])
        by_round.setdefault(rd, {})[r["build"]] = {
            "write": float(r["write_ms"]),
            "read": float(r["read_avg_ms"]),
            "disk": int(r["disk_bytes"]),
        }

write_deltas = []
read_deltas = []
rel_writes, loc_writes = [], []
rel_reads, loc_reads = [], []

for rd in sorted(by_round):
    rel = by_round[rd]["release"]
    loc = by_round[rd]["local"]
    rel_writes.append(rel["write"])
    loc_writes.append(loc["write"])
    rel_reads.append(rel["read"])
    loc_reads.append(loc["read"])
    write_deltas.append((loc["write"] - rel["write"]) / rel["write"] * 100.0)
    read_deltas.append((loc["read"] - rel["read"]) / rel["read"] * 100.0)


def percentile(vals, p):
    if not vals:
        return float("nan")
    s = sorted(vals)
    k = (len(s) - 1) * p / 100.0
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] * (c - k) + s[c] * (k - f)


def stat_block(name, rel_vals, loc_vals, deltas):
    med_rel = median(rel_vals)
    med_loc = median(loc_vals)
    med_delta = median(deltas)
    p95_delta = percentile(deltas, 95)
    wins = sum(1 for d in deltas if d < 0)
    return {
        "name": name,
        "rel_med": med_rel,
        "loc_med": med_loc,
        "rel_min": min(rel_vals),
        "rel_max": max(rel_vals),
        "loc_min": min(loc_vals),
        "loc_max": max(loc_vals),
        "delta_med": med_delta,
        "delta_p95": p95_delta,
        "wins": wins,
        "rounds": len(deltas),
    }


w = stat_block("write", rel_writes, loc_writes, write_deltas)
r = stat_block("read", rel_reads, loc_reads, read_deltas)

def rows_per_s(ms):
    return rows / (ms / 1000.0)


def verdict(delta_med, metric):
    if abs(delta_med) < 3.0:
        return f"INCONCLUSIVE on {metric} (median delta {delta_med:+.1f}% — within ~3% noise band)"
    if delta_med < 0:
        return f"LOCAL FASTER on {metric} (median {delta_med:+.1f}%)"
    return f"LOCAL SLOWER on {metric} (median {delta_med:+.1f}% — possible regression)"


lines = [
    "",
    f"Aggregated over {rounds} paired rounds",
    "",
    "Per-round write (ms):",
    f"  release: {rel_writes}",
    f"  local:   {loc_writes}",
    f"  delta %: {[f'{d:+.1f}' for d in write_deltas]}",
    "",
    "Per-round read avg (ms):",
    f"  release: {rel_reads}",
    f"  local:   {loc_reads}",
    f"  delta %: {[f'{d:+.1f}' for d in read_deltas]}",
    "",
    "Write statistics:",
    f"  release median: {w['rel_med']:.0f} ms  (min {w['rel_min']:.0f}, max {w['rel_max']:.0f})",
    f"  local   median: {w['loc_med']:.0f} ms  (min {w['loc_min']:.0f}, max {w['loc_max']:.0f})",
    f"  local vs release median: {w['delta_med']:+.1f}%",
    f"  local vs release p95:    {w['delta_p95']:+.1f}%",
    f"  local wins (faster):     {w['wins']}/{w['rounds']} rounds",
    f"  release throughput med:  {rows_per_s(w['rel_med']):,.0f} rows/s",
    f"  local   throughput med:  {rows_per_s(w['loc_med']):,.0f} rows/s",
    "",
    "Read statistics:",
    f"  release median: {r['rel_med']:.1f} ms  (min {r['rel_min']:.1f}, max {r['rel_max']:.1f})",
    f"  local   median: {r['loc_med']:.1f} ms  (min {r['loc_min']:.1f}, max {r['loc_max']:.1f})",
    f"  local vs release median: {r['delta_med']:+.1f}%",
    f"  local vs release p95:    {r['delta_p95']:+.1f}%",
    f"  local wins (faster):     {r['wins']}/{r['rounds']} rounds",
    "",
    "Verdict:",
    f"  {verdict(w['delta_med'], 'write')}",
    f"  {verdict(r['delta_med'], 'read')}",
    "",
]
text = "\n".join(lines)
print(text)
Path(out_path).write_text(Path(out_path).read_text() + text + "\n")
PY

echo ""
echo "Results: $RESULTS"
echo "Round CSV: $CSV"
