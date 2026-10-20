"""Summarise a rebalance-storm run: pauses, duplicates, gaps and latency.

Usage: python analyze.py run/
Writes run/results.json and prints a table. From LaserData, Apache License 2.0.
"""
import glob
import json
import os
import sys
from collections import defaultdict


def pct(values, q):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def main(run: str) -> None:
    events = [json.loads(l) for l in open(os.path.join(run, "events.jsonl"))]
    churn = [e for e in events if e["event"] in ("leave", "kill", "join")]
    t0 = next(e["t"] for e in events if e["event"] == "churn-start")
    sent = json.load(open(os.path.join(run, "producer.json")))["sent"]

    rows = []
    for path in glob.glob(os.path.join(run, "c*.jsonl")):
        rows += [json.loads(l) for l in open(path)]
    rows.sort(key=lambda r: r["recv"])

    seen = defaultdict(int)
    for r in rows:
        seen[r["seq"]] += 1
    duplicates = sum(n - 1 for n in seen.values() if n > 1)
    missing = sent - len(seen)

    # A pause is the longest time a partition delivered nothing, after the
    # baseline. With a steady producer, every partition should deliver every few ms.
    by_partition = defaultdict(list)
    for r in rows:
        by_partition[r["p"]].append(r["recv"])
    base_gaps, churn_gaps = [], []
    for times in by_partition.values():
        for a, b in zip(times, times[1:]):
            (churn_gaps if a >= t0 else base_gaps).append((b - a) / 1000)

    lat = lambda rs: [(r["recv"] - r["sent"]) / 1000 for r in rs]
    base, during = [r for r in rows if r["recv"] < t0], [r for r in rows if r["recv"] >= t0]

    result = {
        "sent": sent,
        "unique_delivered": len(seen),
        "missing": missing,
        "duplicates": duplicates,
        "duplicate_rate": duplicates / max(1, sent),
        "membership_changes": len(churn),
        "max_partition_pause_ms": {"baseline": max(base_gaps, default=None), "churn": max(churn_gaps, default=None)},
        "p99_partition_gap_ms": {"baseline": pct(base_gaps, 0.99), "churn": pct(churn_gaps, 0.99)},
        "end_to_end_ms": {
            "baseline": {"p50": pct(lat(base), 0.5), "p99": pct(lat(base), 0.99)},
            "churn": {"p50": pct(lat(during), 0.5), "p99": pct(lat(during), 0.99), "max": pct(lat(during), 1.0)},
        },
    }
    json.dump(result, open(os.path.join(run, "results.json"), "w"), indent=2)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "run")
