"""Shared helpers for the LaserData footprint tests.

Every test talks to the server through the Laser SDK. The sampler process reads
memory and CPU from the kernel (/proc and cgroup v2), so the numbers do not
depend on the client or the server reporting on themselves.
"""
from __future__ import annotations

import json
import os
import struct
import time
from pathlib import Path

import laser_sdk as ls

# Laser Stack's default credentials. Override only to point at another LaserData deployment.
CONNECTION = os.environ.get("LASER_CONNECTION_STRING", "iggy:laser@127.0.0.1:8090")
OUT = Path(os.environ.get("FOOTPRINT_OUT", "results/local"))
SCALE = float(os.environ.get("FOOTPRINT_SCALE", "1"))  # 0.1 for a quick trial run
MESSAGE_BYTES = 1024  # 1 KiB records, the size LaserData's own benchmark suite uses
BATCH = 100  # records per send, also from LaserData's benchmark suite
SEQ = struct.Struct("<Q")
_PAD = os.urandom(MESSAGE_BYTES - SEQ.size)


def payload(seq: int) -> bytes:
    """A 1 KiB record whose first 8 bytes are its sequence number."""
    return SEQ.pack(seq) + _PAD


def seq_of(data: bytes) -> int:
    return SEQ.unpack_from(data)[0]


async def connect() -> ls.Laser:
    return await ls.Laser.connect(CONNECTION)


def scaled(n: float) -> int:
    return max(1, int(n * SCALE))


def _clock() -> float:
    return time.time()


def set_phase(test: str, phase: str) -> None:
    """Tell the sampler which test and phase the next samples belong to."""
    OUT.mkdir(parents=True, exist_ok=True)
    tmp = OUT / "phase.tmp"
    tmp.write_text(f"{test}\t{phase}")
    tmp.replace(OUT / "phase")
    event(test, "phase", phase=phase)


def register_client(role: str) -> None:
    """Ask the sampler to track this process's memory and CPU under `role`."""
    pids = OUT / "pids"
    pids.mkdir(parents=True, exist_ok=True)
    (pids / role).write_text(str(os.getpid()))


def unregister_client(role: str) -> None:
    try:
        (OUT / "pids" / role).unlink()
    except FileNotFoundError:
        pass


def event(test: str, name: str, **fields) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / "events.jsonl", "a") as f:
        f.write(json.dumps({"t": _clock(), "test": test, "event": name, **fields}) + "\n")


def write_result(name: str, data: dict) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{name}.json").write_text(json.dumps(data, indent=2))


class SeqTracker:
    """Counts every record once, so a run with lost or duplicated records is caught."""

    def __init__(self, expected: int):
        self.expected = expected
        self.seen = bytearray(expected)
        self.unique = 0
        self.duplicates = 0
        self.out_of_range = 0

    def add(self, seq: int) -> None:
        if seq >= self.expected:
            self.out_of_range += 1
            return
        if self.seen[seq]:
            self.duplicates += 1
        else:
            self.seen[seq] = 1
            self.unique += 1

    def missing(self, upto: int | None = None) -> int:
        n = self.expected if upto is None else min(upto, self.expected)
        return n - sum(self.seen[:n])
