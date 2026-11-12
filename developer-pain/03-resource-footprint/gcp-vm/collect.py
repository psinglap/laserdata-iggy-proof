"""Joins the two VMs' evidence for one test into the layout merge.py expects.

The client VM's run has the test events and client samples; the server VM's sampler has the
server and backend samples but no test phases. Each server sample gets the phase the client
had announced at that moment (both VMs keep time with Google's NTP).

Usage: python collect.py <client out> <server out> <dest dir>
"""
import bisect
import csv
import json
import shutil
import sys
from pathlib import Path

client, server, dest = map(Path, sys.argv[1:4])
dest.mkdir(parents=True, exist_ok=True)
for f in client.iterdir():
    if f.is_file() and f.name not in ("samples.csv", "phase", "stop"):
        shutil.copy(f, dest / f.name)
events = [json.loads(l) for l in (client / "events.jsonl").read_text().splitlines() if l.strip()]
marks = sorted((e["t"], e["test"], e.get("phase", e["test"])) for e in events if e["event"] == "phase")
times = [m[0] for m in marks]
rows = list(csv.DictReader(open(client / "samples.csv")))
fields = ["t", "test", "phase", "target", "rss_mib", "anon_mib", "file_mib", "cgroup_mib", "cpu_pct"]
for r in csv.DictReader(open(server / "samples.csv")):
    i = bisect.bisect_right(times, float(r["t"])) - 1
    if i < 0:
        r["test"], r["phase"] = "setup", "setup"
    else:
        r["test"], r["phase"] = marks[i][1], marks[i][2]
    rows.append(r)
rows.sort(key=lambda r: float(r["t"]))
with open(dest / "samples.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
print(dest, len(rows), "samples")
