"""Turns footprint.csv into the table we publish: median and p95 per phase.

The first 30 seconds of each phase are dropped as warm-up.
"""
import csv, statistics, sys
from collections import defaultdict

rows = defaultdict(list)
first = {}
for r in csv.DictReader(open(sys.argv[1] if len(sys.argv) > 1 else "footprint.csv")):
    ph, ts = r["phase"], int(r["ts"])
    first.setdefault(ph, ts)
    if ts - first[ph] >= 30:
        rows[ph].append((float(r["rss_mib"]), float(r["cpu_pct"])))

def p95(xs):
    xs = sorted(xs)
    return xs[int(0.95 * (len(xs) - 1))]

print("| Phase | Samples | RSS median (MiB) | RSS p95 (MiB) | CPU median (%) | CPU p95 (%) |")
print("|---|---|---|---|---|---|")
for ph, v in rows.items():
    rss, cpu = [a for a, _ in v], [b for _, b in v]
    print(f"| {ph} | {len(v)} | {statistics.median(rss):.0f} | {p95(rss):.0f} | {statistics.median(cpu):.0f} | {p95(cpu):.0f} |")
