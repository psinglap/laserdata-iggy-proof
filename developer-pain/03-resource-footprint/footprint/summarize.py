"""Turns the per-run evidence in results/rep-*/ into results/summary.json and results/README.md.

Every number is the median across repetitions, with the lowest and highest beside it.
Nothing is estimated: each figure is read from samples.csv (kernel counters, once a second)
or from the tests' own JSON (record counts, timings).

Usage: python summarize.py results [--run-url URL]
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

PARTITIONS = (2, 10, 50)


def load_samples(rep: Path) -> list[dict]:
    rows = []
    with open(rep / "samples.csv") as f:
        for r in csv.DictReader(f):
            for k in ("t", "rss_mib", "anon_mib", "file_mib", "cgroup_mib", "cpu_pct"):
                r[k] = float(r[k]) if r.get(k) not in (None, "") else None
            rows.append(r)
    return rows


def load_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def values(rows, key, **match):
    return [r[key] for r in rows if r[key] is not None and all(r.get(k) == v for k, v in match.items())]


def peak(xs):
    return round(max(xs), 1) if xs else None


def med(xs):
    return round(statistics.median(xs), 1) if xs else None


def slope_per_hour(points):
    """Least-squares slope of (t, value) in units per hour."""
    if len(points) < 10:
        return None
    n = len(points)
    mt = sum(t for t, _ in points) / n
    mv = sum(v for _, v in points) / n
    den = sum((t - mt) ** 2 for t, _ in points)
    return round(sum((t - mt) * (v - mv) for t, v in points) / den * 3600, 1) if den else None


def one_rep(rep: Path) -> dict:
    rows = load_samples(rep)
    out: dict = {"status": load_json(rep / "status.json"), "server_state": load_json(rep / "server-state.json")}

    idle = [r for r in rows if r["test"] == "idle"]
    idle = idle[len(idle) // 4:]  # skip the first quarter after start-up
    out["idle"] = {
        "server_rss_mib": med(values(idle, "rss_mib", target="server")),
        "server_container_mib": med(values(idle, "cgroup_mib", target="server")),
        "server_cpu_pct": med(values(idle, "cpu_pct", target="server")),
        "backend_rss_mib": med(values(idle, "rss_mib", target="plane")),
    }

    backlog = {}
    for p in PARTITIONS:
        phase = f"consume-{p}p"
        res = load_json(rep / f"consumer_backlog-{p}p.json")
        backlog[str(p)] = {
            "consumer_rss_peak_mib": peak(values(rows, "rss_mib", phase=phase, target="consumer")),
            "consumer_rss_median_mib": med(values(rows, "rss_mib", phase=phase, target="consumer")),
            "consumer_cpu_median_pct": med(values(rows, "cpu_pct", phase=phase, target="consumer")),
            "server_rss_peak_mib": peak(values(rows, "rss_mib", phase=phase, target="server")),
            "server_anon_peak_mib": peak(values(rows, "anon_mib", phase=phase, target="server")),
            "server_container_peak_mib": peak(values(rows, "cgroup_mib", phase=phase, target="server")),
            "server_cpu_median_pct": med(values(rows, "cpu_pct", phase=phase, target="server")),
            "records": res.get("records"), "missing": res.get("missing"), "duplicates": res.get("duplicates"),
            "drain_seconds": res.get("seconds"), "records_per_second": res.get("records_per_second"),
        }
    out["consumer_backlog"] = backlog

    prod = load_json(rep / "producer_pause-producer.json")
    cons = load_json(rep / "producer_pause-consumer.json")
    pr = [r for r in rows if r["test"] == "producer_pause" and r["target"] == "producer"]
    before = [r for r in pr if r["phase"] == "steady-before"]
    warm = before[0]["t"] + 60 if before else 0
    before_pts = [(r["t"], r["rss_mib"]) for r in before if r["t"] >= warm and r["rss_mib"] is not None]
    after = [r for r in pr if r["phase"] == "after-resume"]
    after_pts = [(r["t"], r["rss_mib"]) for r in after if r["rss_mib"] is not None]
    late_after = after_pts[len(after_pts) // 2:]
    out["producer_pause"] = {
        "producer_rss_before_median_mib": med([v for _, v in before_pts]),
        "producer_rss_during_pause_peak_mib": peak(values(pr, "rss_mib", phase="server-paused")),
        "producer_rss_after_peak_mib": peak([v for _, v in after_pts]),
        "producer_rss_last_minutes_median_mib": med([v for _, v in after_pts[-120:]]),
        "producer_growth_before_mib_per_hour": slope_per_hour(before_pts),
        "producer_growth_after_mib_per_hour": slope_per_hour(late_after),
        "consumer_rss_peak_mib": peak(values(rows, "rss_mib", test="producer_pause", target="consumer")),
        "server_rss_peak_mib": peak(values(rows, "rss_mib", test="producer_pause", target="server")),
        "records_sent": prod.get("records_sent"), "failed_writes": prod.get("failed_writes"),
        "missing": cons.get("missing"), "duplicates": cons.get("duplicates"),
    }

    orch = load_json(rep / "restart-orchestrator.json").get("restarts", [])
    rprod = load_json(rep / "restart-producer.json")
    rcons = load_json(rep / "restart-consumer.json")
    srv = [r for r in rows if r["target"] == "server" and r["test"] == "restart"]
    steady = [r for r in srv if r["phase"] == "steady"]
    restarts = {}
    for i, r in enumerate(orch):
        began = r["began"]
        until = orch[i + 1]["began"] if i + 1 < len(orch) else began + 600  # never reach into the next restart
        window = [s for s in srv if began <= s["t"] <= min(began + 60, until)]
        blocked = rprod.get("outages", []) + rprod.get("stalls", [])
        outage = max((o for o in blocked if began - 1 <= o["to"] < until and o["from"] < until),
                     key=lambda o: o["to"], default=None)
        gap = max((g for g in rcons.get("gaps", []) if began - 1 <= g["to"] < until),
                  key=lambda g: g["to"], default=None)
        restarts[r["kind"]] = {
            "seconds_to_ready": r["seconds_to_ready"],
            "seconds_until_producer_write": round(outage["to"] - began, 1) if outage else 0.0,
            "seconds_until_consumer_record": round(gap["to"] - began, 1) if gap else 0.0,
            "server_rss_peak_60s_mib": peak(values(window, "rss_mib")),
            "server_container_peak_60s_mib": peak(values(window, "cgroup_mib")),
            "server_cpu_peak_60s_pct": peak(values(window, "cpu_pct")),
        }
    out["restart"] = {
        "server_rss_steady_mib": med(values(steady, "rss_mib")),
        "server_cpu_steady_pct": med(values(steady, "cpu_pct")),
        "restarts": restarts,
        "records_confirmed": rcons.get("records_confirmed"),
        "missing_confirmed": rcons.get("missing_confirmed"), "duplicates": rcons.get("duplicates"),
    }
    return out


def combine(reps: list[dict]):
    """Median, min and max across repetitions for every numeric leaf."""
    def walk(nodes):
        first = nodes[0]
        if isinstance(first, dict):
            return {k: walk([n.get(k) for n in nodes if isinstance(n, dict)]) for k in first}
        nums = [n for n in nodes if isinstance(n, (int, float)) and not isinstance(n, bool)]
        if nums and len(nums) == len([n for n in nodes if n is not None]):
            return {"median": round(statistics.median(nums), 1), "min": min(nums), "max": max(nums), "n": len(nums)}
        return nodes
    return walk(reps)


def fmt(cell, unit=""):
    if not isinstance(cell, dict) or "median" not in cell:
        return "n/a"
    m, lo, hi = cell["median"], cell["min"], cell["max"]
    r = f"{m:,.0f}{unit}" if m >= 10 or float(m).is_integer() else f"{m:,.1f}{unit}"
    return r if lo == hi else f"{r} ({lo:,.0f}–{hi:,.0f})"


def readme(s: dict) -> str:
    env, a = s["env"], s["combined"]
    lines = [
        "# Footprint results", "",
        f"Measured {env.get('started', '')[:10]} by [this GitHub Actions run]({s.get('run_url', '')}), "
        f"{s['repetitions']} repetitions, each on a fresh GitHub-hosted machine "
        f"({env.get('host_cores')} vCPU {env.get('host_cpu')}, {env.get('host_memory_mib', 0) // 1024} GiB, kernel {env.get('kernel')}).",
        "",
        f"Server: Laser Streams `{env.get('server_version') or 'see digest'}` "
        f"(`{env.get('server_image_digest', '')}`), from Laser Stack commit `{env.get('laser_stack_commit', '')[:7]}`, "
        f"LaserData's shipped settings, limited to {env.get('server_memory_limit')} of memory and 2 dedicated cores. "
        f"Clients: Laser SDK {env.get('laser_sdk')} for Python, SDK default settings, on separate cores.",
        "",
        "Each cell is the median across repetitions, with the lowest and highest in brackets. Memory is resident memory (RSS) in MiB "
        "unless it says container, which also counts the page cache the kernel keeps for the server's files. CPU is percent of one core.",
        "",
        "## Idle", "",
        "| Server RSS | Server container | Server CPU | Managed backend RSS |", "|---|---|---|---|",
        f"| {fmt(a['idle']['server_rss_mib'])} | {fmt(a['idle']['server_container_mib'])} | {fmt(a['idle']['server_cpu_pct'], '%')} | {fmt(a['idle']['backend_rss_mib'])} |",
        "", "## 1. Consumer reading a 7 million record backlog", "",
        "| Partitions | Consumer RSS peak | Consumer RSS median | Server RSS peak | Server container peak | Drain time (s) | Records/s | Missing |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for p in PARTITIONS:
        b = a["consumer_backlog"][str(p)]
        lines.append(f"| {p} | {fmt(b['consumer_rss_peak_mib'])} | {fmt(b['consumer_rss_median_mib'])} | {fmt(b['server_rss_peak_mib'])} | "
                     f"{fmt(b['server_container_peak_mib'])} | {fmt(b['drain_seconds'])} | {fmt(b['records_per_second'])} | {fmt(b['missing'])} |")
    pp = a["producer_pause"]
    lines += [
        "", "## 2. Producer for 30 minutes at 8,000 records/s, server paused for 60 s halfway", "",
        "| Producer RSS before | Peak while paused | Peak after resume | Last 2 minutes | Growth before pause (MiB/h) | Growth after (MiB/h) | Failed writes | Missing |",
        "|---|---|---|---|---|---|---|---|",
        f"| {fmt(pp['producer_rss_before_median_mib'])} | {fmt(pp['producer_rss_during_pause_peak_mib'])} | {fmt(pp['producer_rss_after_peak_mib'])} | "
        f"{fmt(pp['producer_rss_last_minutes_median_mib'])} | {fmt(pp['producer_growth_before_mib_per_hour'])} | {fmt(pp['producer_growth_after_mib_per_hour'])} | "
        f"{fmt(pp['failed_writes'])} | {fmt(pp['missing'])} |",
    ]
    rs = a["restart"]
    lines += [
        "", "## 3. Server restart with 4 GiB on disk, producer and consumer connected", "",
        f"Steady before restart: server RSS {fmt(rs['server_rss_steady_mib'])}, CPU {fmt(rs['server_cpu_steady_pct'], '%')}.", "",
        "| Restart | Server answers (s) | Producer writes again (s) | Consumer reads again (s) | Server RSS peak, first 60 s | Container peak | CPU peak |",
        "|---|---|---|---|---|---|---|",
    ]
    for kind, r in rs["restarts"].items():
        lines.append(f"| {kind} | {fmt(r['seconds_to_ready'])} | {fmt(r['seconds_until_producer_write'])} | {fmt(r['seconds_until_consumer_record'])} | "
                     f"{fmt(r['server_rss_peak_60s_mib'])} | {fmt(r['server_container_peak_60s_mib'])} | {fmt(r['server_cpu_peak_60s_pct'], '%')} |")
    lines += ["", f"Confirmed records missing after both restarts: {fmt(rs['missing_confirmed'])}.", "",
              "Raw evidence for every repetition is in `rep-*/`: `samples.csv` (one row per second per process), "
              "`events.jsonl`, one JSON per test, `env.json` and the tail of the server log.", ""]
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("results")
    p.add_argument("--run-url", default="")
    args = p.parse_args()
    root = Path(args.results)
    reps = sorted(d for d in root.glob("rep-*") if (d / "samples.csv").exists())
    per_rep = {d.name: one_rep(d) for d in reps}
    summary = {"run_url": args.run_url, "repetitions": len(reps),
               "env": load_json(reps[0] / "env.json") if reps else {},
               "per_repetition": per_rep, "combined": combine(list(per_rep.values()))}
    (root / "summary.json").write_text(json.dumps(summary, indent=2))
    (root / "README.md").write_text(readme(summary))
    print((root / "README.md").read_text())


if __name__ == "__main__":
    main()
