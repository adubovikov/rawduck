# Shared A/B benchmark helpers for run_vs_release*.sh
# shellcheck shell=bash

ab_detect_archive() {
    local version=$1
    local os arch
    os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    arch="$(uname -m)"
    case "$os-$arch" in
        linux-x86_64|linux-amd64) echo "rawduck-linux-amd64-${version}.tar.gz" ;;
        linux-aarch64|linux-arm64) echo "rawduck-linux-arm64-${version}.tar.gz" ;;
        darwin-arm64) echo "rawduck-macos-arm64-${version}.tar.gz" ;;
        darwin-x86_64) echo "rawduck-macos-amd64-${version}.tar.gz" ;;
        *) echo "Unsupported platform: $os $arch" >&2; return 1 ;;
    esac
}

ab_install_release() {
    local cache=$1 version=$2
    local archive dest dir url
    archive="$(ab_detect_archive "$version")"
    dest="$cache/$archive"
    url="https://github.com/quackscience/rawduck/releases/download/${version}/${archive}"

    if [[ -x "$cache/bin/rawduck" ]]; then
        return 0
    fi

    echo "Downloading RawDuck ${version} (${archive})..."
    curl -fsSL "$url" -o "$dest"
    echo "Extracting..."
    tar -xzf "$dest" -C "$cache"

    dir="$(find "$cache" -maxdepth 1 -type d -name 'rawduck-*' | head -1)"
    if [[ -z "$dir" || ! -x "$dir/rawduck" ]]; then
        echo "Release layout unexpected under $cache" >&2
        return 1
    fi

    mkdir -p "$cache/bin/extension/rawduck"
    cp "$dir/duckdb" "$cache/bin/duckdb"
    cp "$dir/rawduck" "$cache/bin/rawduck"
    cp "$dir/extension/rawduck/rawduck.duckdb_extension" "$cache/bin/extension/rawduck/"
    chmod +x "$cache/bin/duckdb" "$cache/bin/rawduck"
}

ab_install_local_wrapper() {
    local cache=$1 local_bin=$2
    mkdir -p "$cache/bin"
    cat > "$cache/bin/rawduck_local" <<EOF
#!/bin/sh
exec "$local_bin" "\$@"
EOF
    chmod +x "$cache/bin/rawduck_local"
}

ab_file_bytes() {
    [[ -f "$1" ]] && stat -c '%s' "$1" || echo 0
}

ab_time_best_of_3() {
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

ab_time_once() {
    local cmd=$1 start end ms
    start=$(date +%s%N)
    eval "$cmd"
    end=$(date +%s%N)
    echo $(( (end - start) / 1000000 ))
}

ab_bench_read_ms() {
    local bin=$1 db=$2 attach=$3 query=$4 best_of=$5
    local best=999999999 i start end ms sql runs
    sql="${attach} ${query}"
    runs=${best_of:-3}
    for ((i = 1; i <= runs; i++)); do
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

ab_read_queries() {
    READ_QUERIES=(
        "SELECT action, count(*) FROM raw.events GROUP BY action"
        "SELECT \"user.name\", count(*) AS n FROM raw.events WHERE action = 'click' GROUP BY 1 ORDER BY n DESC LIMIT 5"
        "SELECT sum(amount) FROM raw.events WHERE amount > 100"
        "SELECT count(DISTINCT \"user.plan\") FROM raw.events"
    )
}

ab_write_rawduck() {
    local bin=$1 raw_db=$2 data=$3
    rm -f "$raw_db" "${raw_db}.wal"
    "$bin" -c "
        ATTACH 'rawduck:${raw_db}' AS raw;
        CALL raw_ingest_file('raw.events', '${data}');
        CHECKPOINT;
    " >/dev/null 2>&1
}

ab_bench_round() {
    # Usage: ab_bench_round label bin raw_db data rows write_mode
    # write_mode: once | best3
    local label=$1 bin=$2 raw_db=$3 data=$4 rows=$5 write_mode=${6:-best3}
    local attach="ATTACH 'rawduck:${raw_db}' AS raw;"
    local write_ms read_sum read_n q ms read_avg disk rows_out cols

    ab_read_queries

    if [[ "$write_mode" == "once" ]]; then
        write_ms=$(ab_time_once "ab_write_rawduck '$bin' '$raw_db' '$data'")
    else
        write_ms=$(ab_time_best_of_3 "ab_write_rawduck '$bin' '$raw_db' '$data'")
    fi

    read_sum=0
    read_n=0
    for q in "${READ_QUERIES[@]}"; do
        ms=$(ab_bench_read_ms "$bin" "" "$attach" "$q" 3)
        read_sum=$((read_sum + ms))
        read_n=$((read_n + 1))
    done
    read_avg=$(awk "BEGIN {printf \"%.1f\", $read_sum / $read_n}")

    disk=$(ab_file_bytes "$raw_db")
    rows_out=$("$bin" -csv -c "${attach} SELECT count(*) FROM raw.events;" 2>/dev/null | tail -1)
    cols=$("$bin" -csv -c "${attach} SELECT count(*) FROM information_schema.columns WHERE table_name='events';" 2>/dev/null | tail -1)

    if [[ "$rows_out" != "$rows" ]]; then
        echo "FAIL: $label row count $rows_out (expected $rows)" >&2
        return 1
    fi

    echo "${write_ms},${read_avg},${disk},${cols}"
}
