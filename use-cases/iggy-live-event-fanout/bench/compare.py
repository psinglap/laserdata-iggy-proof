"""p99 vs number of gateways, one line per system, with the 100 ms budget line.

results.csv columns: system,gateways,p50_ms,p99_ms,p999_ms
"""
import csv
import sys
from collections import defaultdict

import matplotlib.pyplot as plt

rows = defaultdict(list)
src = open(sys.argv[1] if len(sys.argv) > 1 else "results.csv")
for r in csv.DictReader(l for l in src if not l.startswith("#")):
    rows[r["system"]].append((int(r["gateways"]), float(r["p99_ms"]), float(r["p999_ms"])))

fig, ax = plt.subplots(figsize=(8, 4.5))
for system, pts in rows.items():
    pts.sort()
    ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", label=f"{system} p99")
    ax.plot([p[0] for p in pts], [p[2] for p in pts], linestyle="--", alpha=0.5, label=f"{system} p99.9")
ax.axhline(100, color="crimson", linewidth=1)
ax.text(0.02, 108, "100 ms event-to-gateway budget", color="crimson", fontsize=9, transform=ax.get_yaxis_transform())
ax.set_xlabel("Gateways reading every event (consumer groups), 10K events/s, replicated")
ax.set_ylabel("Event-to-gateway latency (ms)")
ax.set_yscale("log")
ax.legend(frameon=False, fontsize=8)
ax.grid(alpha=0.2)
fig.tight_layout()
fig.savefig("p99-vs-gateways.png", dpi=160)
print("wrote p99-vs-gateways.png")
