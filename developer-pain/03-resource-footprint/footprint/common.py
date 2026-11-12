"""Shared helpers for the LaserData footprint tests.

Every test talks to the server through the Laser SDK. The sampler process reads
memory and CPU from the kernel (/proc and cgroup v2), so the numbers do not
depend on the client or the server reporting on themselves.
"""
from __future__ import annotations

import asyncio
import json
import os
import shlex
import struct
import time
from pathlib import Path

import laser_sdk as ls

# Laser Stack's default credentials. Override only to point at another LaserData deployment.
CONNECTION = os.environ.get("LASER_CONNECTION_STRING", "iggy:laser@127.0.0.1:8090")
OUT = Path(os.environ.get("FOOTPRINT_OUT", "results/local"))
SCALE = float(os.environ.get("FOOTPRINT_SCALE", "1"))  # 0.1 for a quick trial run
# How the tests reach the server's Docker: locally, or over SSH when the server is another VM.
DOCKER = shlex.split(os.environ.get("FOOTPRINT_DOCKER", "docker"))
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


def load_result(name: str) -> dict:
    path = OUT / f"{name}.json"
    return json.loads(path.read_text()) if path.exists() else {}


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


def missing_ranges(tracker: SeqTracker, upto: int, limit: int = 20) -> list[list[int]]:
    """The first `limit` runs of sequence numbers below `upto` that never arrived."""
    out, start = [], None
    for i in range(min(upto, tracker.expected)):
        if not tracker.seen[i]:
            start = i if start is None else start
        elif start is not None:
            out.append([start, i - 1])
            start = None
            if len(out) >= limit:
                return out
    if start is not None:
        out.append([start, min(upto, tracker.expected) - 1])
    return out[:limit]


async def consume_live(group, tracker: SeqTracker, finished, test: str, silence_s: float = 20.0) -> dict:
    """Reads with a Laser SDK group consumer, the way a long-running service would.

    Every error is recorded. If no record arrives for `silence_s` while the producer is still
    running, the consumer is shut down and built again, and that is counted, so the report
    shows whether the SDK consumer resumed on its own after an outage or needed help.
    `finished()` returns the confirmed total once the producer is done, else None.
    """
    errors: dict[str, int] = {}
    recreations: list[float] = []
    gaps: list[dict] = []

    async def build():
        consumer = group.consumer(polling="first")
        await consumer.init()
        return consumer

    consumer = await build()
    last_record = time.time()
    idle_since = None
    while True:
        message = None
        try:
            message = await consumer.next_within(2000)
        except ls.TimeoutError:
            pass
        except Exception as error:
            key = f"{type(error).__name__}: {str(error)[:140]}"
            if key not in errors:
                event(test, "consumer_error", error=key)
            errors[key] = errors.get(key, 0) + 1
            await asyncio.sleep(0.2)
        now = time.time()
        if message is not None:
            if now - last_record > 0.5:
                gaps.append({"from": last_record, "to": now})
            last_record = now
            tracker.add(seq_of(message.payload))
            idle_since = None
            continue
        total = finished()
        if total is not None:
            if tracker.unique >= total:
                break
            idle_since = idle_since or now
            if now - idle_since > 60:
                break
        if now - last_record > silence_s and (not recreations or now - recreations[-1] > silence_s):
            recreations.append(now)
            event(test, "consumer_recreated", silent_seconds=round(now - last_record, 1))
            try:
                await consumer.shutdown()
            except Exception:
                pass
            try:
                consumer = await build()
            except Exception as error:
                key = f"rebuild {type(error).__name__}: {str(error)[:120]}"
                errors[key] = errors.get(key, 0) + 1
    try:
        await consumer.shutdown()
    except Exception as error:
        key = f"shutdown {type(error).__name__}: {str(error)[:120]}"
        errors[key] = errors.get(key, 0) + 1
    return {"errors": errors, "recreations": recreations, "gaps": gaps}
