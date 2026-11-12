"""Test 3: server CPU and memory through a restart, with data on disk and clients connected.

The problem: restarting the broker sent CPU and memory so high, for so long, that the
services around it stalled.

What this does:
  preload      writes 4 million records (1 KiB each, about 4 GiB) so the server has real
               data to reopen
  produce      a Laser SDK producer keeps sending 10,000 records/s and retries through outages
  consume      a Laser SDK consumer group keeps reading and checks every record
  orchestrate  60 s steady, then a graceful restart (`docker restart`), 120 s, then a crash
               (`docker kill -s KILL` and `docker start`), 120 s
The sampler records the server's memory and CPU every second. The run reports, for each
restart, seconds until the server answers again, seconds until the producer's next write
succeeds and the consumer's next record arrives, and peak server memory and CPU in the
first 60 seconds. It fails if any record the producer had confirmed is missing.

Usage: python restart.py preload
       python restart.py consume &  python restart.py produce &  python restart.py orchestrate
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.request

import common as c

TEST = "restart"
STREAM, TOPIC = "fp-restart", "events"
PARTITIONS = 8
RATE = 10_000
SERVER = os.environ.get("FOOTPRINT_SERVER", "laser-stack-iggy-1")
PING = os.environ.get("FOOTPRINT_PING", "http://127.0.0.1:3000/ping")
STOP = c.OUT / "restart_stop"
TOTAL = c.OUT / "restart_total"
PRELOAD = c.scaled(4_000_000)
LIVE_MAX = PRELOAD + RATE * 900  # upper bound on live records, for the tracker


async def preload() -> None:
    c.set_phase(TEST, "preload")
    laser = await c.connect()
    try:
        producer = laser.stream(STREAM).topic(TOPIC).producer(
            partitions=PARTITIONS, batch_length=c.BATCH, create_stream=True, never_expire=True)
        await producer.init()
        for first in range(0, PRELOAD, c.BATCH):
            await producer.send_batch([c.payload(s) for s in range(first, first + c.BATCH)])
    finally:
        await laser.close()
    c.event(TEST, "preloaded", records=PRELOAD)


async def produce() -> int:
    """Sends at RATE, and after any failure retries the same batch until it is confirmed."""
    c.register_client("producer")
    seq = PRELOAD
    outages, stalls = [], []
    laser = await c.connect()
    try:
        producer = laser.stream(STREAM).topic(TOPIC).producer(partitions=PARTITIONS, batch_length=c.BATCH)
        await producer.init()
        start, sent_since = time.perf_counter(), 0
        while not STOP.exists():
            batch = [c.payload(s) for s in range(seq, seq + c.BATCH)]
            failed_at = None
            sent_at = time.time()
            while True:
                try:
                    await producer.send_batch(batch)
                    took = time.time() - sent_at
                    if took > 0.25:  # the SDK retried inside its own timeout: count it as a stall
                        stalls.append({"from": sent_at, "to": time.time()})
                        c.event(TEST, "producer_stall", seconds=round(took, 2))
                    break
                except Exception as error:
                    if failed_at is None:
                        failed_at = time.time()
                        c.event(TEST, "producer_error", error=repr(error)[:200])
                    await asyncio.sleep(0.2)
            if failed_at is not None:
                outages.append({"from": failed_at, "to": time.time()})
                c.event(TEST, "producer_resumed", seconds=round(time.time() - failed_at, 2))
                start, sent_since = time.perf_counter(), 0  # no catch-up burst after an outage
            seq += c.BATCH
            sent_since += c.BATCH
            ahead = start + sent_since / RATE - time.perf_counter()
            if ahead > 0:
                await asyncio.sleep(ahead)
    finally:
        await laser.close()
        c.unregister_client("producer")
    TOTAL.write_text(str(seq))
    c.write_result(f"{TEST}-producer", {"records_confirmed": seq, "outages": outages, "stalls": stalls})
    return 0


async def consume() -> int:
    c.register_client("consumer")
    tracker = c.SeqTracker(LIVE_MAX)
    gaps = []
    laser = await c.connect()
    try:
        group = laser.stream(STREAM).topic(TOPIC).consumer_group("footprint")
        consumer = group.consumer(polling="first")
        await consumer.init()
        last_record, idle_since = time.time(), None
        while True:
            try:
                message = await consumer.next_within(2000)
            except Exception as error:
                message = None
                if "imeout" not in type(error).__name__ + str(error):
                    c.event(TEST, "consumer_error", error=repr(error)[:200])
                    try:
                        await consumer.shutdown()
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)
                    try:
                        consumer = group.consumer(polling="first")
                        await consumer.init()
                    except Exception:
                        await asyncio.sleep(0.5)
            if message is not None:
                now = time.time()
                if now - last_record > 0.5:
                    gaps.append({"from": last_record, "to": now})
                    c.event(TEST, "consumer_resumed", seconds=round(now - last_record, 2))
                last_record = now
                tracker.add(c.seq_of(message.payload))
                idle_since = None
                continue
            if TOTAL.exists():
                total = int(TOTAL.read_text())
                if tracker.unique >= total:
                    break
                idle_since = idle_since or time.time()
                if time.time() - idle_since > 60:
                    break
        try:
            await consumer.shutdown()
        except Exception:
            pass
    finally:
        await laser.close()
        c.unregister_client("consumer")
    total = int(TOTAL.read_text())
    result = {"records_confirmed": total, "received_unique": total - tracker.missing(total),
              "missing_confirmed": tracker.missing(total), "duplicates": tracker.duplicates, "gaps": gaps}
    c.write_result(f"{TEST}-consumer", result)
    print({k: v for k, v in result.items() if k != "gaps"})
    return 0 if result["missing_confirmed"] == 0 else 1


def ping_ok() -> bool:
    try:
        with urllib.request.urlopen(PING, timeout=1) as response:
            return response.status == 200
    except Exception:
        return False


def wait_ready(since: float) -> float:
    while not ping_ok():
        time.sleep(0.1)
    return round(time.time() - since, 2)


def orchestrate() -> int:
    STOP.unlink(missing_ok=True)
    TOTAL.unlink(missing_ok=True)
    c.set_phase(TEST, "steady")
    time.sleep(c.scaled(60))
    restarts = []
    for kind in ("graceful", "crash"):
        c.set_phase(TEST, f"{kind}-restart")
        began = time.time()
        if kind == "graceful":
            subprocess.run(["docker", "restart", "-t", "30", SERVER], check=True)
        else:
            subprocess.run(["docker", "kill", "-s", "KILL", SERVER], check=True)
            subprocess.run(["docker", "start", SERVER], check=True)
        ready = wait_ready(began)
        c.event(TEST, "restart", kind=kind, began=began, seconds_to_ready=ready)
        restarts.append({"kind": kind, "began": began, "seconds_to_ready": ready})
        c.set_phase(TEST, f"after-{kind}")
        time.sleep(c.scaled(120))
    STOP.write_text("1")
    c.write_result(f"{TEST}-orchestrator", {"restarts": restarts})
    print(json.dumps(restarts))
    return 0


async def cleanup() -> None:
    laser = await c.connect()
    try:
        await laser.stream(STREAM).delete()
    finally:
        await laser.close()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("step", choices=["preload", "produce", "consume", "orchestrate", "cleanup"])
    step = p.parse_args().step
    if step == "orchestrate":
        return orchestrate()
    return asyncio.run({"preload": preload, "produce": produce, "consume": consume,
                        "cleanup": cleanup}[step]()) or 0


if __name__ == "__main__":
    sys.exit(main())
