#!/usr/bin/env python3
"""Generate OTLP/JSON export envelopes for RawDuck benchmark (from BENCHMARK.md)."""
import json
import os
import random
import sys

random.seed(11)
SVC = ["checkout", "cart", "payments", "search", "auth", "inventory", "shipping", "frontend"]
RT = ["/api/v1/orders", "/api/v1/cart", "/api/v1/pay", "/api/v1/search", "/login", "/health"]


def kv(k, v):
    if isinstance(v, bool):
        return {"key": k, "value": {"boolValue": v}}
    if isinstance(v, int):
        return {"key": k, "value": {"intValue": str(v)}}
    if isinstance(v, float):
        return {"key": k, "value": {"doubleValue": v}}
    return {"key": k, "value": {"stringValue": str(v)}}


def res(s):
    return {
        "attributes": [
            kv("service.name", s),
            kv("deployment.environment", "production"),
            kv("cloud.region", "us-east-1"),
            kv("host.name", "pod-%d" % random.randint(1, 400)),
        ]
    }


def span(ts):
    st = random.choice([200, 200, 200, 201, 400, 404, 500])
    d = random.randint(2 * 10**5, 8 * 10**8)
    return {
        "traceId": os.urandom(16).hex(),
        "spanId": os.urandom(8).hex(),
        "name": random.choice(RT),
        "kind": random.randint(1, 5),
        "startTimeUnixNano": str(ts),
        "endTimeUnixNano": str(ts + d),
        "attributes": [
            kv("http.method", random.choice(["GET", "POST", "PUT", "DELETE"])),
            kv("http.route", random.choice(RT)),
            kv("http.status_code", st),
            kv("retry", random.choice([True, False])),
        ],
        "status": {"code": 2 if st >= 500 else 1},
    }


def log_rec(ts):
    return {
        "timeUnixNano": str(ts),
        "severityNumber": random.choice([9, 13, 17]),
        "severityText": random.choice(["INFO", "WARN", "ERROR"]),
        "body": {"stringValue": random.choice(["ok", "timeout", "retry", "fail"])},
        "attributes": [
            kv("service.name", random.choice(SVC)),
            kv("http.status_code", random.choice([200, 404, 500])),
        ],
    }


def metric(ts):
    return {
        "name": random.choice(["cpu.util", "mem.used", "req.latency"]),
        "sum": {
            "dataPoints": [
                {
                    "timeUnixNano": str(ts),
                    "asDouble": random.random() * 100,
                    "attributes": [kv("service.name", random.choice(SVC))],
                }
            ]
        },
    }


def gen(name, total, per, rec, wrap, out_dir):
    ts = 1_700_000_000_000_000_000
    w = 0
    path = os.path.join(out_dir, name + ".ndjson")
    with open(path, "w") as f:
        while w < total:
            n = min(per, total - w)
            f.write(
                json.dumps(wrap(random.choice(SVC), [rec(ts + (w + j) * 1000) for j in range(n)]))
                + "\n"
            )
            w += n
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"Wrote {path}: {total} records, {size_mb:.1f} MB")


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "."
    gen(
        "traces",
        n,
        80,
        span,
        lambda s, r: {"resourceSpans": [{"resource": res(s), "scopeSpans": [{"spans": r}]}]},
        out_dir,
    )
    gen(
        "logs",
        n,
        100,
        log_rec,
        lambda s, r: {
            "resourceLogs": [{"resource": res(s), "scopeLogs": [{"logRecords": r}]}]
        },
        out_dir,
    )
    gen(
        "metrics",
        n,
        100,
        metric,
        lambda s, r: {
            "resourceMetrics": [{"resource": res(s), "scopeMetrics": [{"metrics": r}]}]
        },
        out_dir,
    )


if __name__ == "__main__":
    main()
