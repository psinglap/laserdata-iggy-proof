"""A desk that joins late: replays from N seconds ago, checks for gaps, then goes live."""
import argparse
import asyncio
import os
import time
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import DEFAULT_URL, STREAM, TOPIC, unpack


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    since_us = int((time.time() - a.since) * 1_000_000)
    consumer = await client.consumer_group(
        f"late-{os.getpid()}", STREAM, TOPIC,
        polling_strategy=PollingStrategy.Timestamp(since_us),
        batch_length=5000,
        auto_commit=AutoCommit.Disabled(),
        poll_interval=timedelta(milliseconds=1),
    )
    last, gaps, n, t0, stop = {}, 0, 0, time.monotonic(), asyncio.Event()

    async def on_tick(m):
        nonlocal gaps, n
        sym, seq, sent_ns, _, _ = unpack(m.payload())
        prev = last.get(sym, seq - 1)
        if seq <= prev:      # re-read after a restart (at-least-once): skip the duplicate
            return
        if seq > prev + 1:   # a hole in this symbol's sequence
            gaps += 1
        last[sym], n = seq, n + 1
        if time.time_ns() - sent_ns < 50_000_000:  # within 50 ms of now: caught up
            dt = time.monotonic() - t0
            print(f"caught up: replayed {n:,} ticks from the last {a.since}s in {dt:.2f}s "
                  f"({n / dt:,.0f} ticks/s), {len(last)} symbols, {gaps} gaps")
            stop.set()

    await consumer.consume_messages(on_tick, stop)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--since", type=int, default=300, help="seconds of history to replay")
    asyncio.run(main(p.parse_args()))
