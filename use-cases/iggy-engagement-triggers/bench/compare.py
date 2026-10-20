"""Draw the one chart the post needs: end-to-end p99 vs rate, with the 5 ms budget line.

results.csv columns: system,rate_msgs,p50_ms,p99_ms,p999_ms  (one row per system per rate step)
"""
import csv
import sys
from collections import defaultdict

import matplotlib.pyplot as plt

rows = defaultdict(list)
src = open(sys.argv[1] if len(sys.argv) > 1 else "results.csv")
for r in csv.DictReader(l for l in src if not l.startswith("#")):
    rows[r["system"]].append((int(r["rate_msgs"]), float(r["p99_ms"]), float(r["p999_ms"])))

fig, ax = plt.subplots(figsize=(8, 4.5))
for system, pts in rows.items():
    pts.sort()
    x = [p[0] / 1e6 for p in pts]
    ax.plot(x, [p[1] for p in pts], marker="o", label=f"{system} p99")
    ax.plot(x, [p[2] for p in pts], linestyle="--", alpha=0.5, label=f"{system} p99.9")
ax.axhline(50, color="crimson", linewidth=1)
ax.text(0.02, 53, "50 ms broker budget (of 1 s in-session)", color="crimson", fontsize=9,
        transform=ax.get_yaxis_transform())
ax.set_xlabel("Events per second (millions), replicated, 300 B, 5 readers")
ax.set_ylabel("End-to-end latency (ms)")
ax.set_yscale("log")
ax.legend(frameon=False, fontsize=8)
ax.grid(alpha=0.2)
fig.tight_layout()
fig.savefig("engagement-p99-vs-rate.png", dpi=160)
print("wrote engagement-p99-vs-rate.png")
