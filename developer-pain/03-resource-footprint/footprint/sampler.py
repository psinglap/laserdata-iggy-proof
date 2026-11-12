"""Samples memory and CPU once a second from the Linux kernel.

Targets:
  server   the Laser Streams container (laser-stack `iggy` service)
  plane    the LaserData managed backend container (laser-stack `plane` service)
  <role>   every Laser SDK client process a test registers under results/<run>/pids/

For containers it records the server process's resident memory (VmRSS), the
container's anonymous memory and page cache (cgroup memory.stat) and the
container total (memory.current, the number a memory limit enforces). CPU is
in percent of one core, so 200 means two cores fully busy.

Usage: python sampler.py --out results/rep-1 --server <container> --plane <container>
Stops when the file <out>/stop exists.
"""
from __future__ import annotations

import argparse
import csv
import os
import subprocess
import time
from pathlib import Path

CLK = os.sysconf("SC_CLK_TCK")
MIB = 1024 * 1024


def read(path: str | Path) -> str | None:
    try:
        return Path(path).read_text()
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None


def proc_rss_mib(pid: int) -> float | None:
    status = read(f"/proc/{pid}/status")
    if not status:
        return None
    for line in status.splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) / 1024
    return None


def proc_cpu_seconds(pid: int) -> float | None:
    stat = read(f"/proc/{pid}/stat")
    if not stat:
        return None
    fields = stat.rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / CLK  # utime + stime


class Container:
    def __init__(self, name: str, process: str):
        self.name, self.process = name, process
        self.cgroup: Path | None = None
        self.last: tuple[float, int] | None = None

    def _resolve(self) -> None:
        cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", self.name],
                             capture_output=True, text=True).stdout.strip()
        if not cid:
            return
        for candidate in (f"/sys/fs/cgroup/system.slice/docker-{cid}.scope",
                          f"/sys/fs/cgroup/docker/{cid}"):
            if Path(candidate, "memory.current").exists():
                self.cgroup = Path(candidate)
                return

    def sample(self) -> dict:
        if self.cgroup is None or not (self.cgroup / "memory.current").exists():
            self._resolve()
        if self.cgroup is None:
            return {}
        current = read(self.cgroup / "memory.current")
        stat = read(self.cgroup / "memory.stat")
        cpu = read(self.cgroup / "cpu.stat")
        procs = read(self.cgroup / "cgroup.procs")
        if not current or not stat or not cpu:
            return {}
        mem = {k: int(v) for k, v in (l.split() for l in stat.splitlines())}
        usage = int(next(l.split()[1] for l in cpu.splitlines() if l.startswith("usage_usec")))
        now = time.monotonic()
        cpu_pct = None
        if self.last and usage >= self.last[1]:
            cpu_pct = (usage - self.last[1]) / 1e6 / (now - self.last[0]) * 100
        self.last = (now, usage)
        # The named server process; if the name differs in an image, the largest process
        # in the container (the init shim is tiny), so the number is never silently missing.
        rss, largest = None, 0.0
        for pid in (procs or "").split():
            value = proc_rss_mib(int(pid)) or 0.0
            if (read(f"/proc/{pid}/comm") or "").strip() == self.process:
                rss = value
                break
            largest = max(largest, value)
        if rss is None:
            rss = largest or None
        return {"rss_mib": rss, "anon_mib": mem.get("anon", 0) / MIB,
                "file_mib": mem.get("file", 0) / MIB,
                "cgroup_mib": int(current) / MIB, "cpu_pct": cpu_pct}


class Client:
    def __init__(self, pid: int):
        self.pid = pid
        self.last: tuple[float, float] | None = None

    def sample(self) -> dict:
        rss, cpu = proc_rss_mib(self.pid), proc_cpu_seconds(self.pid)
        if rss is None or cpu is None:
            return {}
        now = time.monotonic()
        cpu_pct = None
        if self.last:
            cpu_pct = (cpu - self.last[1]) / (now - self.last[0]) * 100
        self.last = (now, cpu)
        return {"rss_mib": rss, "cpu_pct": cpu_pct}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--server", required=True)
    p.add_argument("--plane", required=True)
    args = p.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    targets = {"server": Container(args.server, "iggy-server"),
               "plane": Container(args.plane, "laser-plane")}
    clients: dict[str, Client] = {}
    fields = ["t", "test", "phase", "target", "rss_mib", "anon_mib", "file_mib", "cgroup_mib", "cpu_pct"]
    with open(out / "samples.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        next_tick = time.monotonic()
        while not (out / "stop").exists():
            test, _, phase = (read(out / "phase") or "setup\tsetup").partition("\t")
            pid_dir = out / "pids"
            live = {}
            if pid_dir.exists():
                for role_file in pid_dir.iterdir():
                    pid = int(role_file.read_text() or 0)
                    key = f"{role_file.name}"
                    if key not in clients or clients[key].pid != pid:
                        clients[key] = Client(pid)
                    live[key] = clients[key]
            now = time.time()
            for name, target in list(targets.items()) + list(live.items()):
                row = target.sample()
                if row:
                    w.writerow({"t": round(now, 3), "test": test, "phase": phase, "target": name,
                                **{k: (round(v, 2) if isinstance(v, float) else v) for k, v in row.items()}})
            f.flush()
            next_tick += 1
            time.sleep(max(0, next_tick - time.monotonic()))


if __name__ == "__main__":
    main()
