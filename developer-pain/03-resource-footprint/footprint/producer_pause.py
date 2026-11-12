"""Test 2: producer memory over a long run, with the server frozen for 60 seconds halfway.

The problem: a producer's memory kept climbing under sustained sends because records piled
up in client-side buffers, and the only workaround was to shrink those buffers by hand.

What this does:
  produce  one Laser SDK producer in background (buffered) mode, with the SDK's default
           buffer budget, sends 20,000 records/s (1 KiB each) for 30 minutes. Halfway, the
           server container is paused (`docker pause`) for 60 seconds, so every send has
           nowhere to go and the client buffer fills.
  consume  a Laser SDK consumer group reads the same topic live and checks every record.
The sampler records both processes' memory every second. The run reports memory before,
during and after the pause, and fails if any record the producer accepted is missing.

Usage: python producer_pause.py consume &   python producer_pause.py produce
"""
from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import threading
import time

import common as c
import laser_sdk as ls

TEST = "producer_pause"
STREAM, TOPIC = "fp-pause", "events"
RATE = 20_000
PARTITIONS = 8
PAUSE_SECONDS = 60
SERVER = os.environ.get("FOOTPRINT_SERVER", "laser-stack-iggy-1")


def planned_seconds() -> int:
    return c.scaled(30 * 60)


def planned_records() -> int:
    return RATE * planned_seconds()


def pause_server(after_seconds: float) -> None:
    time.sleep(after_seconds)
    c.set_phase(TEST, "server-paused")
    subprocess.run(c.DOCKER + ["pause", SERVER], check=True)
    c.event(TEST, "server_paused")
    time.sleep(PAUSE_SECONDS)
    subprocess.run(c.DOCKER + ["unpause", SERVER], check=True)
    c.set_phase(TEST, "after-resume")
    c.event(TEST, "server_resumed")


async def produce() -> int:
    c.register_client("producer")
    seconds, total = planned_seconds(), planned_records()
    failures: list[dict] = []
    reported: list[int] = []  # sequence numbers the SDK reported to the app as not confirmed

    def on_error(error) -> None:
        detail = {"error": repr(error)[:300]}
        for field in ("committed", "unconfirmed"):
            items = getattr(error, field, None) or []
            detail[field] = items if isinstance(items, int) else len(items)
            if field == "unconfirmed" and not isinstance(items, int):
                for item in items:
                    data = item if isinstance(item, (bytes, bytearray)) else getattr(item, "payload", None)
                    if data:
                        reported.append(c.seq_of(bytes(data)))
        failures.append(detail)

    laser = await c.connect()
    try:
        topic = laser.stream(STREAM).topic(TOPIC)
        producer = topic.producer(
            partitions=PARTITIONS, create_stream=True,
            expire_after_ms=10 * 60 * 1000,  # 10 minutes of retention keeps the runner's disk in bounds
            background=ls.BackgroundConfig(error_callback=on_error))  # SDK default buffer budget
        await producer.init()
        c.set_phase(TEST, "steady-before")
        start = time.perf_counter()
        # The pause runs on its own thread: a send blocked on a full buffer must not
        # be able to stop the server from being unpaused.
        pauser = threading.Thread(target=pause_server, args=(seconds / 2,), daemon=True)
        pauser.start()
        sent = 0
        while sent < total:
            await producer.send_batch([c.payload(s) for s in range(sent, sent + c.BATCH)])
            sent += c.BATCH
            ahead = start + sent / RATE - time.perf_counter()
            if ahead > 0:
                await asyncio.sleep(ahead)
        pauser.join()
        try:
            await producer.shutdown()  # flushes the buffer
        except Exception as error:
            on_error(error)
        took = time.perf_counter() - start
    finally:
        await laser.close()
        c.unregister_client("producer")
    (c.OUT / "pause_total").write_text(str(total))
    result = {"records_sent": total, "rate_target": RATE, "seconds": round(took, 1),
              "pause_seconds": PAUSE_SECONDS, "failed_writes": len(failures), "failures": failures[:10],
              "records_reported_unconfirmed": sum(f.get("unconfirmed", 0) for f in failures),
              "reported_unconfirmed_seqs": sorted(reported),
              "producer_settings": "Laser SDK background mode, default buffer budget, block when full"}
    c.write_result(f"{TEST}-producer", result)
    print(result)
    return 0


async def consume() -> int:
    c.register_client("consumer")
    total = planned_records()
    tracker = c.SeqTracker(total)
    marker = c.OUT / "pause_total"
    marker.unlink(missing_ok=True)
    laser = await c.connect()
    try:
        topic = laser.stream(STREAM).topic(TOPIC)
        await topic.ensure(PARTITIONS)
        live = await c.consume_live(topic.consumer_group("footprint"), tracker,
                                    lambda: total if marker.exists() else None, TEST)
    finally:
        await laser.close()
        c.unregister_client("consumer")
    missing = [i for i, seen in enumerate(tracker.seen) if not seen]
    reported = set(c.load_result(f"{TEST}-producer").get("reported_unconfirmed_seqs", []))
    result = {"records_expected": total, "received_unique": tracker.unique,
              "duplicates": tracker.duplicates, "missing": len(missing),
              # A record is only lost if it is missing AND the SDK never told the app it failed.
              "missing_not_reported": len([s for s in missing if s not in reported]),
              "missing_ranges": c.missing_ranges(tracker, total),
              "consumer_errors": live["errors"], "consumer_recreated": len(live["recreations"]),
              "gaps": live["gaps"]}
    c.write_result(f"{TEST}-consumer", result)
    print(result)
    return 0 if result["missing_not_reported"] == 0 else 1


async def cleanup() -> None:
    laser = await c.connect()
    try:
        await laser.stream(STREAM).delete()
    finally:
        await laser.close()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["produce", "consume", "cleanup"])
    step = p.parse_args().step
    if step == "produce":
        return asyncio.run(produce())
    if step == "consume":
        return asyncio.run(consume())
    asyncio.run(cleanup())
    return 0


if __name__ == "__main__":
    sys.exit(main())
