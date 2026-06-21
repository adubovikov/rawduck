#!/usr/bin/env bash
# CI gate: run multi-round A/B vs release and fail if local write regresses too much.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MAX_PCT="${WRITE_REGRESSION_MAX_PCT:-10}"
ROUNDS="${ROUNDS:-3}"

if [[ ! -x "$ROOT/build/release/duckdb" ]]; then
  echo "Missing release build. Run: GEN=ninja make release" >&2
  exit 1
fi

export ROUNDS
"$ROOT/scripts/run_vs_release_stats.sh" 50000

python3 - "$ROOT/agent_planning/bench/vs_release/ab_stats_rounds.csv" "$MAX_PCT" <<'PY'
import csv
import sys
from statistics import median

csv_path, max_pct = sys.argv[1], float(sys.argv[2])
by_round = {}
with open(csv_path, newline="") as f:
    for row in csv.DictReader(f):
        rd = int(row["round"])
        by_round.setdefault(rd, {})[row["build"]] = float(row["write_ms"])

deltas = []
for rd in sorted(by_round):
    rel = by_round[rd]["release"]
    loc = by_round[rd]["local"]
    deltas.append((loc - rel) / rel * 100.0)

med = median(deltas)
wins = sum(1 for d in deltas if d < 0)
print(f"Write delta per round: {[f'{d:+.1f}%' for d in deltas]}")
print(f"Median write delta (local vs release): {med:+.1f}%")
print(f"Local faster in {wins}/{len(deltas)} rounds")
print(f"Threshold: local may not be more than {max_pct:.0f}% slower (median)")

if med > max_pct:
    print(f"FAIL: median write regression {med:+.1f}% exceeds +{max_pct:.0f}%")
    sys.exit(1)

print("PASS: write benchmark within threshold")
PY
