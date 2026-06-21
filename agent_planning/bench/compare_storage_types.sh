#!/usr/bin/env bash
set -uo pipefail

DUCKDB="${DUCKDB:-$(dirname "$0")/../../build/release/duckdb}"
NDJSON_100K="/tmp/rawduck_compare_100k.ndjson"
NDJSON_500K="/tmp/rawduck_compare_500k.ndjson"
ROUNDS="${ROUNDS:-5}"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

CSV_OPTS="columns={'line':'VARCHAR'}, auto_detect=false, header=false, delim=E'\\\\x01', quote=''"

measure_ms() {
    local start end
    start=$(date +%s%N)
    eval "$1"
    end=$(date +%s%N)
    echo $(( (end - start) / 1000000 ))
}

best_of() {
    local cmd="$1" rounds="$2" best=999999
    for ((r=1; r<=rounds; r++)); do
        local ms
        ms=$(measure_ms "$cmd")
        if (( ms < best )); then best=$ms; fi
    done
    echo "$best"
}

disk_size() {
    local db="$1"
    local total=0
    for f in "$db" "$db.wal"; do
        if [ -f "$f" ]; then
            total=$((total + $(stat -c%s "$f")))
        fi
    done
    echo $total
}

ddb() {
    local db="$1"
    shift
    echo "$@" | "$DUCKDB" "$db" >/dev/null 2>&1
}

ddb_val() {
    local db="$1"
    shift
    echo "$@" | "$DUCKDB" "$db" -csv -noheader 2>/dev/null | head -1
}

run_benchmark() {
    local label="$1" ndjson="$2" rows="$3"
    local db

    echo ""
    echo "=== $label ($rows rows) ==="
    echo ""

    # --- RawDuck shredded ---
    db="$TMPDIR/rawduck_${rows}.duckdb"
    local rawduck_write
    rawduck_write=$(best_of "rm -f '$db' '$db.wal'; ddb '$db' \"FROM raw_ingest_file('events', '$ndjson');
CHECKPOINT;\"" "$ROUNDS")

    local rawduck_disk rawduck_count
    rawduck_disk=$(disk_size "$db")
    rawduck_count=$(ddb_val "$db" "SELECT count(*) FROM events;")

    local rawduck_read_filter rawduck_read_group rawduck_read_scan
    rawduck_read_filter=$(best_of "ddb '$db' \"SELECT count(*) FROM events WHERE score > 50;\"" "$ROUNDS")
    rawduck_read_group=$(best_of "ddb '$db' \"SELECT tag, count(*), avg(score) FROM events GROUP BY tag;\"" "$ROUNDS")
    rawduck_read_scan=$(best_of "ddb '$db' \"SELECT count(*), sum(id) FROM events;\"" "$ROUNDS")

    # --- JSON column ---
    db="$TMPDIR/json_${rows}.duckdb"
    local json_write
    json_write=$(best_of "rm -f '$db' '$db.wal'; ddb '$db' \"CREATE TABLE events (data JSON);
INSERT INTO events SELECT line::JSON FROM read_csv('$ndjson', $CSV_OPTS);
CHECKPOINT;\"" "$ROUNDS")

    local json_disk json_count
    json_disk=$(disk_size "$db")
    json_count=$(ddb_val "$db" "SELECT count(*) FROM events;")

    local json_read_filter json_read_group json_read_scan
    json_read_filter=$(best_of "ddb '$db' \"SELECT count(*) FROM events WHERE (data->>'score')::DOUBLE > 50;\"" "$ROUNDS")
    json_read_group=$(best_of "ddb '$db' \"SELECT data->>'tag' as tag, count(*), avg((data->>'score')::DOUBLE) FROM events GROUP BY tag;\"" "$ROUNDS")
    json_read_scan=$(best_of "ddb '$db' \"SELECT count(*), sum((data->>'id')::BIGINT) FROM events;\"" "$ROUNDS")

    # --- VARCHAR column ---
    db="$TMPDIR/varchar_${rows}.duckdb"
    local varchar_write
    varchar_write=$(best_of "rm -f '$db' '$db.wal'; ddb '$db' \"CREATE TABLE events (data VARCHAR);
INSERT INTO events SELECT line FROM read_csv('$ndjson', $CSV_OPTS);
CHECKPOINT;\"" "$ROUNDS")

    local varchar_disk varchar_count
    varchar_disk=$(disk_size "$db")
    varchar_count=$(ddb_val "$db" "SELECT count(*) FROM events;")

    local varchar_read_filter varchar_read_group varchar_read_scan
    varchar_read_filter=$(best_of "ddb '$db' \"SELECT count(*) FROM events WHERE (data::JSON->>'score')::DOUBLE > 50;\"" "$ROUNDS")
    varchar_read_group=$(best_of "ddb '$db' \"SELECT data::JSON->>'tag' as tag, count(*), avg((data::JSON->>'score')::DOUBLE) FROM events GROUP BY tag;\"" "$ROUNDS")
    varchar_read_scan=$(best_of "ddb '$db' \"SELECT count(*), sum((data::JSON->>'id')::BIGINT) FROM events;\"" "$ROUNDS")

    # --- Print results ---
    echo "Row counts: RawDuck=$rawduck_count  JSON=$json_count  VARCHAR=$varchar_count"
    echo ""

    local rw_rps jw_rps vw_rps
    rw_rps=$(python3 -c "print(f'{$rows * 1000 / max($rawduck_write, 1):,.0f}')")
    jw_rps=$(python3 -c "print(f'{$rows * 1000 / max($json_write, 1):,.0f}')")
    vw_rps=$(python3 -c "print(f'{$rows * 1000 / max($varchar_write, 1):,.0f}')")

    local rd_kb jd_kb vd_kb
    rd_kb=$(python3 -c "print(f'{$rawduck_disk/1024:.0f}')")
    jd_kb=$(python3 -c "print(f'{$json_disk/1024:.0f}')")
    vd_kb=$(python3 -c "print(f'{$varchar_disk/1024:.0f}')")

    printf "| %-12s | %9s | %12s | %10s | %10s | %10s | %10s |\n" \
           "Storage" "Write ms" "Rows/s" "Filter ms" "Group ms" "Scan ms" "Disk KB"
    printf "|%s|%s|%s|%s|%s|%s|%s|\n" \
           "--------------" "-----------" "--------------" "------------" "------------" "------------" "------------"
    printf "| %-12s | %9d | %12s | %10d | %10d | %10d | %10s |\n" \
           "RawDuck" "$rawduck_write" "$rw_rps" \
           "$rawduck_read_filter" "$rawduck_read_group" "$rawduck_read_scan" "$rd_kb"
    printf "| %-12s | %9d | %12s | %10d | %10d | %10d | %10s |\n" \
           "JSON col" "$json_write" "$jw_rps" \
           "$json_read_filter" "$json_read_group" "$json_read_scan" "$jd_kb"
    printf "| %-12s | %9d | %12s | %10d | %10d | %10d | %10s |\n" \
           "VARCHAR col" "$varchar_write" "$vw_rps" \
           "$varchar_read_filter" "$varchar_read_group" "$varchar_read_scan" "$vd_kb"

    echo ""

    local json_write_pct varchar_write_pct
    json_write_pct=$(python3 -c "print(f'{($rawduck_write - $json_write) / max($json_write, 1) * 100:+.1f}%')")
    varchar_write_pct=$(python3 -c "print(f'{($rawduck_write - $varchar_write) / max($varchar_write, 1) * 100:+.1f}%')")

    echo "Write overhead vs opaque:"
    echo "  RawDuck vs JSON:    $json_write_pct"
    echo "  RawDuck vs VARCHAR: $varchar_write_pct"
    echo ""

    if (( rawduck_read_filter > 0 && json_read_filter > 0 )); then
        local filter_speedup group_speedup scan_speedup
        filter_speedup=$(python3 -c "print(f'{$json_read_filter / max($rawduck_read_filter, 1):.1f}x')")
        group_speedup=$(python3 -c "print(f'{$json_read_group / max($rawduck_read_group, 1):.1f}x')")
        scan_speedup=$(python3 -c "print(f'{$json_read_scan / max($rawduck_read_scan, 1):.1f}x')")
        echo "Read speedup (RawDuck vs JSON ->> extraction):"
        echo "  Filter:  ${filter_speedup} faster"
        echo "  Group:   ${group_speedup} faster"
        echo "  Scan:    ${scan_speedup} faster"
    fi
}

echo "# RawDuck vs Opaque Storage Comparison"
echo "# $(date -Iseconds)"
echo "# Rounds: $ROUNDS (best of N)"
echo "# DuckDB: $DUCKDB"

run_benchmark "100k rows, 6 cols" "$NDJSON_100K" 100000
run_benchmark "500k rows, 6 cols" "$NDJSON_500K" 500000
