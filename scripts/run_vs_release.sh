#!/usr/bin/env bash
# A/B benchmark: local build vs official RawDuck release (default v0.0.2).
# Same methodology as https://github.com/adubovikov/rawjsonduck-tests test_compare_storage.sh:
# best of 3 for write/read, on-disk size after CHECKPOINT.
#
# Usage:
#   ./scripts/run_vs_release.sh              # 50,000 rows
#   ./scripts/run_vs_release.sh 100000       # custom row count
#   RELEASE_VERSION=v0.0.3 ./scripts/run_vs_release.sh
#
# Requires: curl, python3, bash, a release build (GEN=ninja make release).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ROWS="${1:-50000}"
RELEASE_VERSION="${RELEASE_VERSION:-v0.0.2}"
CACHE="$ROOT/agent_planning/bench/.cache/release"
DATA="$ROOT/agent_planning/bench/data/compare_events.ndjson"
OUT_DIR="$ROOT/agent_planning/bench/vs_release"
RESULTS="$OUT_DIR/ab_results.txt"
LOCAL_BIN="$ROOT/build/release/duckdb"

mkdir -p "$CACHE" "$OUT_DIR"

detect_archive() {
    local os arch
    os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    arch="$(uname -m)"
    case "$os-$arch" in
        linux-x86_64|linux-amd64) echo "rawduck-linux-amd64-${RELEASE_VERSION}.tar.gz" ;;
        linux-aarch64|linux-arm64) echo "rawduck-linux-arm64-${RELEASE_VERSION}.tar.gz" ;;
        darwin-arm64) echo "rawduck-macos-arm64-${RELEASE_VERSION}.tar.gz" ;;
        darwin-x86_64) echo "rawduck-macos-amd64-${RELEASE_VERSION}.tar.gz" ;;
        *) echo "Unsupported platform: $os $arch" >&2; exit 1 ;;
    esac
}

install_release() {
    local archive dest dir url
    archive="$(detect_archive)"
    dest="$CACHE/$archive"
    url="https://github.com/quackscience/rawduck/releases/download/${RELEASE_VERSION}/${archive}"

    if [[ -x "$CACHE/bin/rawduck" ]]; then
        return 0
    fi

    echo "Downloading RawDuck ${RELEASE_VERSION} (${archive})..."
    curl -fsSL "$url" -o "$dest"
    echo "Extracting..."
    tar -xzf "$dest" -C "$CACHE"

    # Tarballs unpack to rawduck-linux-amd64/ (or arm64), not *-v0.0.2/
    dir="$(find "$CACHE" -maxdepth 1 -type d -name 'rawduck-*' | head -1)"
    if [[ -z "$dir" || ! -x "$dir/rawduck" ]]; then
        echo "Release layout unexpected under $CACHE" >&2
        exit 1
    fi

    mkdir -p "$CACHE/bin/extension/rawduck"
    cp "$dir/duckdb" "$CACHE/bin/duckdb"
    cp "$dir/rawduck" "$CACHE/bin/rawduck"
    cp "$dir/extension/rawduck/rawduck.duckdb_extension" "$CACHE/bin/extension/rawduck/"
    chmod +x "$CACHE/bin/duckdb" "$CACHE/bin/rawduck"
    echo "Release binary: $CACHE/bin/rawduck"
}

install_local_wrapper() {
    mkdir -p "$CACHE/bin"
    cat > "$CACHE/bin/rawduck_local" <<EOF
#!/bin/sh
exec "$LOCAL_BIN" "\$@"
EOF
    chmod +x "$CACHE/bin/rawduck_local"
}

require_local_build() {
    if [[ ! -x "$LOCAL_BIN" ]]; then
        echo "Local build missing. Run: cd $ROOT && GEN=ninja make release" >&2
        exit 1
    fi
}

file_bytes() {
    [[ -f "$1" ]] && stat -c '%s' "$1" || echo 0
}

time_best_of_3() {
    local cmd=$1
    local best=999999999 i start end ms
    for i in 1 2 3; do
        start=$(date +%s%N)
        eval "$cmd"
        end=$(date +%s%N)
        ms=$(( (end - start) / 1000000 ))
        (( ms < best )) && best=$ms
    done
    echo "$best"
}

bench_read_ms() {
    local bin=$1 db=$2 attach=$3 query=$4
    local best=999999999 i start end ms sql
    sql="${attach} ${query}"
    for i in 1 2 3; do
        start=$(date +%s%N)
        if [[ -n "$db" ]]; then
            "$bin" "$db" -csv -c "$sql" >/dev/null 2>&1
        else
            "$bin" -csv -c "$sql" >/dev/null 2>&1
        fi
        end=$(date +%s%N)
        ms=$(( (end - start) / 1000000 ))
        (( ms < best )) && best=$ms
    done
    echo "$best"
}

bench_label() {
    local label=$1 bin=$2
    local raw_db="$OUT_DIR/${label}_rawduck.db"
    local attach="ATTACH 'rawduck:${raw_db}' AS raw;"
    local -a read_queries=(
        "SELECT action, count(*) FROM raw.events GROUP BY action"
        "SELECT \"user.name\", count(*) AS n FROM raw.events WHERE action = 'click' GROUP BY 1 ORDER BY n DESC LIMIT 5"
        "SELECT sum(amount) FROM raw.events WHERE amount > 100"
        "SELECT count(DISTINCT \"user.plan\") FROM raw.events"
    )

    echo "" >&2
    echo "==> Benchmark: $label ($bin)" >&2

    write_rawduck() {
        rm -f "$raw_db" "${raw_db}.wal"
        "$bin" -c "
            ATTACH 'rawduck:${raw_db}' AS raw;
            CALL raw_ingest_file('raw.events', '${DATA}');
            CHECKPOINT;
        " >/dev/null 2>&1
    }

    local write_ms read_sum read_avg read_n q ms
    write_ms=$(time_best_of_3 write_rawduck)
    echo "    write (best of 3): ${write_ms} ms" >&2

    read_sum=0
    read_n=0
    for q in "${read_queries[@]}"; do
        ms=$(bench_read_ms "$bin" "" "$attach" "$q")
        read_sum=$((read_sum + ms))
        read_n=$((read_n + 1))
    done
    read_avg=$(awk "BEGIN {printf \"%.1f\", $read_sum / $read_n}")

    local disk rows cols
    disk=$(file_bytes "$raw_db")
    rows=$("$bin" -csv -c "${attach} SELECT count(*) FROM raw.events;" 2>/dev/null | tail -1)
    cols=$("$bin" -csv -c "${attach} SELECT count(*) FROM information_schema.columns WHERE table_name='events';" 2>/dev/null | tail -1)

    if [[ "$rows" != "$ROWS" ]]; then
        echo "FAIL: $label row count $rows (expected $ROWS)" >&2
        exit 1
    fi

    echo "    read avg (best of 3 × ${read_n} queries): ${read_avg} ms" >&2
    echo "    disk: ${disk} bytes | cols: ${cols}" >&2

    echo "${label},${write_ms},${read_avg},${disk},${rows},${cols}"
}

require_local_build
install_release
install_local_wrapper
python3 "$ROOT/scripts/gen_compare_data.py" "$ROWS" "$DATA" >/dev/null

SOURCE_BYTES=$(file_bytes "$DATA")
SOURCE_MB=$(awk "BEGIN {printf \"%.2f\", $SOURCE_BYTES / 1048576}")

CSV="$OUT_DIR/ab_metrics.csv"
echo "label,write_ms,read_avg_ms,disk_bytes,rows,cols" >"$CSV"

{
    echo "# RawDuck A/B: local build vs ${RELEASE_VERSION}"
    echo "# $(date -Iseconds)"
    echo "# Rows: $ROWS | NDJSON: ${SOURCE_MB} MB ($SOURCE_BYTES bytes)"
    echo "# Local: $LOCAL_BIN"
    echo "# Release: $CACHE/bin/rawduck"
    echo ""
} | tee "$RESULTS"

release_line=$(bench_label "release" "$CACHE/bin/rawduck")
local_line=$(bench_label "local" "$CACHE/bin/rawduck_local")

echo "$release_line" >>"$CSV"
echo "$local_line" >>"$CSV"

python3 - "$CSV" "$ROWS" "$RELEASE_VERSION" "$RESULTS" <<'PY'
import csv
import sys
from pathlib import Path

csv_path, rows, version, out_path = sys.argv[1:5]
rows = int(rows)
with open(csv_path, newline="") as f:
    data = {r["label"]: r for r in csv.DictReader(f)}

rel = data["release"]
loc = data["local"]

def f(x):
    return float(x)

w_rel, w_loc = f(rel["write_ms"]), f(loc["write_ms"])
r_rel, r_loc = f(rel["read_avg_ms"]), f(loc["read_avg_ms"])
d_rel, d_loc = int(rel["disk_bytes"]), int(loc["disk_bytes"])
t_rel = rows / (w_rel / 1000.0)
t_loc = rows / (w_loc / 1000.0)
w_delta = (w_loc - w_rel) / w_rel * 100.0
r_delta = (r_loc - r_rel) / r_rel * 100.0

lines = [
    "",
    "Summary — RawDuck only (best of 3, same as rawjsonduck-tests)",
    "",
    f"| Build | Write (ms) | Rows/s | Read avg (ms) | Disk (bytes) |",
    f"|-------|------------|--------|---------------|--------------|",
    f"| {version} | {w_rel:.0f} | {t_rel:,.0f} | {r_rel:.1f} | {d_rel} |",
    f"| local | {w_loc:.0f} | {t_loc:,.0f} | {r_loc:.1f} | {d_loc} |",
    "",
    f"Write: {w_delta:+.1f}% ({'faster' if w_delta < 0 else 'slower'} local vs release)",
    f"Read:  {r_delta:+.1f}% ({'faster' if r_delta < 0 else 'slower'} local vs release)",
    f"Disk:  {d_loc - d_rel:+d} bytes",
    "",
]
text = "\n".join(lines)
print(text)
Path(out_path).write_text(Path(out_path).read_text() + text + "\n")
PY

echo ""
echo "Results: $RESULTS"
echo "Metrics CSV: $CSV"
