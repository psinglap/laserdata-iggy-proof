"""Rebalance-storm test for Apache Iggy consumer groups.

A producer writes numbered messages at a steady rate. A pool of consumers in one
group reads them, and a controller changes the group every few seconds: a member
leaves cleanly, a member is killed, or a new member joins. Every delivery is logged
so analyze.py can measure pauses, duplicates and gaps.

From LaserData, which builds and runs Apache Iggy. Apache License 2.0.
"""
import argparse
import asyncio
import json
import multiprocessing as mp
import os
import random
import signal
import time
from datetime import timedelta

from apache_iggy import (
    AutoCommit,
    AutoCommitAfter,
    IggyClient,
    PollingStrategy,
    SendMessage,
)

STREAM, TOPIC, GROUP = "storm", "events", "storm-group"


def now_us() -> int:
    return time.time_ns() // 1_000


async def produce(url: str, partitions: int, rate: int, seconds: int, out: str) -> None:
    client = IggyClient.from_connection_string(url)
    await client.connect()
    producer = await client.producer(STREAM, TOPIC, topic_partitions_count=partitions)
    sent, start = 0, time.monotonic()
    async with producer:
        while time.monotonic() - start < seconds:
            # Send in 10 ms ticks so the rate stays steady under load.
            due = int((time.monotonic() - start) * rate)
            batch = [SendMessage(f"{seq}:{now_us()}") for seq in range(sent, due)]
            if batch:
                await producer.send(batch)
                sent = due
            await asyncio.sleep(0.01)
    with open(out, "w") as f:
        json.dump({"sent": sent, "seconds": seconds, "rate": rate}, f)


async def consume(url: str, name: str, log_dir: str, work_ms: float) -> None:
    client = IggyClient.from_connection_string(url)
    await client.connect()
    consumer = await client.consumer_group(
        GROUP,
        STREAM,
        TOPIC,
        polling_strategy=PollingStrategy.Next(),
        # Commit after each message is handled: at-least-once, the usual choice.
        auto_commit=AutoCommit.After(AutoCommitAfter.ConsumingEachMessage()),
        batch_length=100,
        poll_interval=timedelta(milliseconds=1),
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    log = open(os.path.join(log_dir, f"{name}.jsonl"), "a", buffering=1)

    async def on_message(message) -> None:
        seq, sent_us = message.payload().decode().split(":")
        if work_ms:
            await asyncio.sleep(work_ms / 1000)
        log.write(json.dumps({
            "c": name, "p": message.partition_id(), "o": message.offset(),
            "seq": int(seq), "sent": int(sent_us), "recv": now_us(),
        }) + "\n")

    await consumer.consume_messages(on_message, stop)
    # Graceful exit: leave the group so the server reassigns now, not after the session timeout.
    await client.leave_consumer_group(STREAM, TOPIC, GROUP)
    log.close()


def _run_consumer(url: str, name: str, log_dir: str, work_ms: float) -> None:
    asyncio.run(consume(url, name, log_dir, work_ms))


def _run_producer(url: str, partitions: int, rate: int, seconds: int, out: str) -> None:
    asyncio.run(produce(url, partitions, rate, seconds, out))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--url", default="iggy+tcp://iggy:iggy@127.0.0.1:8090")
    ap.add_argument("--partitions", type=int, default=12)
    ap.add_argument("--consumers", type=int, default=4, help="members at the start")
    ap.add_argument("--rate", type=int, default=5000, help="messages per second")
    ap.add_argument("--seconds", type=int, default=300)
    ap.add_argument("--warmup", type=int, default=30, help="seconds with no churn, as the baseline")
    ap.add_argument("--every", type=float, default=10.0, help="seconds between membership changes")
    ap.add_argument("--mode", choices=["leave", "kill", "freeze", "join", "mixed"], default="mixed")
    ap.add_argument("--work-ms", type=float, default=0.0, help="simulated handling time per message")
    ap.add_argument("--out", default="run")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    events = open(os.path.join(args.out, "events.jsonl"), "w", buffering=1)
    note = lambda kind, who: events.write(json.dumps({"t": now_us(), "event": kind, "consumer": who}) + "\n")

    ctx = mp.get_context("spawn")
    pool, frozen, counter = {}, [], 0

    def start_one() -> None:
        nonlocal counter
        name = f"c{counter}"
        counter += 1
        proc = ctx.Process(target=_run_consumer, args=(args.url, name, args.out, args.work_ms))
        proc.start()
        pool[name] = proc
        note("join", name)

    for _ in range(args.consumers):
        start_one()
    time.sleep(2)  # let the group settle before the producer starts

    producer = ctx.Process(
        target=_run_producer,
        args=(args.url, args.partitions, args.rate, args.seconds, os.path.join(args.out, "producer.json")))
    producer.start()
    note("producer-start", "-")
    time.sleep(args.warmup)
    note("churn-start", "-")

    rng = random.Random(7)
    end = time.monotonic() + args.seconds - args.warmup - 10
    while time.monotonic() < end:
        mode = rng.choice(["leave", "kill", "freeze", "join"]) if args.mode == "mixed" else args.mode
        if mode == "join" or len(pool) <= 1:
            start_one()
        else:
            name = rng.choice(sorted(pool))
            proc = pool.pop(name)
            if mode == "kill":
                proc.kill()  # SIGKILL: no leave call, but the OS closes the socket
            elif mode == "freeze":
                os.kill(proc.pid, signal.SIGSTOP)  # a hung host: socket open, no pings
                frozen.append(proc)
            else:
                proc.terminate()  # SIGTERM: the consumer leaves the group cleanly
            note(mode, name)
            start_one()  # keep the group size steady
        time.sleep(args.every)
    note("churn-end", "-")

    producer.join()
    time.sleep(15)  # let consumers drain what is left
    for proc in frozen:
        proc.kill()
    for proc in pool.values():
        proc.terminate()
    for proc in list(pool.values()) + frozen:
        proc.join(10)
    print(f"done; logs in {args.out}/, now run: python analyze.py {args.out}")


if __name__ == "__main__":
    main()
