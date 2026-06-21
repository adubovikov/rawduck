#!/usr/bin/env python3
"""Generate NDJSON for the storage comparison / A/B benchmark."""
import json
import random
import sys
from pathlib import Path

OUT = Path(sys.argv[2]) if len(sys.argv) > 2 else Path(__file__).resolve().parent.parent / "agent_planning" / "bench" / "data" / "compare_events.ndjson"
ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 50000

random.seed(42)
actions = ["click", "view", "purchase", "login", "logout"]
plans = ["basic", "pro", "enterprise"]

OUT.parent.mkdir(parents=True, exist_ok=True)
with open(OUT, "w") as f:
    for i in range(ROWS):
        obj = {
            "id": i,
            "action": random.choice(actions),
            "ts": "2024-01-15T10:30:00",
            "user": {
                "name": f"user_{i % 500}",
                "plan": random.choice(plans),
            },
            "amount": round(random.uniform(1, 500), 2),
        }
        f.write(json.dumps(obj) + "\n")

print(f"Wrote {ROWS} rows to {OUT}")
