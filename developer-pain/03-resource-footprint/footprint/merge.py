"""Merges the per-machine evidence of a parallel run into one folder per repetition.

The workflow runs each test on its own machine. Their artifacts arrive as
incoming/rep-<N>-<test>/; this writes results/rep-<N>/ with one samples.csv and
events.jsonl, every test's JSON, a combined status.json, and one env.json per
machine (env-<test>.json) next to a representative env.json.

Usage: python merge.py incoming results
"""
from __future__ import annotations

import json
import re
import shutil
import sys
from pathlib import Path

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
groups: dict[str, list[Path]] = {}
for d in sorted(src.glob("rep-*")):
    m = re.match(r"(rep-\d+)-(.+)", d.name)
    if m and (d / "samples.csv").exists():
        groups.setdefault(m.group(1), []).append(d)

for rep, parts in groups.items():
    out = dst / rep
    out.mkdir(parents=True, exist_ok=True)
    status, states = {}, {}
    with open(out / "samples.csv", "w") as samples, open(out / "events.jsonl", "w") as events:
        for i, part in enumerate(parts):
            test = part.name.split("-", 2)[2]
            lines = (part / "samples.csv").read_text().splitlines(keepends=True)
            samples.writelines(lines if i == 0 else lines[1:])
            if (part / "events.jsonl").exists():
                events.write((part / "events.jsonl").read_text())
            for f in part.glob("*.json"):
                if f.name == "status.json":
                    status.update(json.loads(f.read_text()))
                elif f.name == "server-state.json":
                    states[test] = json.loads(f.read_text())
                elif f.name == "env.json":
                    shutil.copy(f, out / f"env-{test}.json")
                    if not (out / "env.json").exists():
                        shutil.copy(f, out / "env.json")
                else:
                    shutil.copy(f, out / f.name)
            log = part / "server.log"
            if log.exists():
                (out / f"server-tail-{test}.log").write_text("".join(log.read_text().splitlines(keepends=True)[-300:]))
    (out / "status.json").write_text(json.dumps(status, indent=2))
    (out / "server-state.json").write_text(json.dumps(states, indent=2))
    print(rep, "←", ", ".join(p.name for p in parts), "|", status)
