"""A desk (risk, P&L, pricing, surveillance): one consumer group on the tick topic.

Prints rolling send-to-receive latency every second and counts sequence gaps.
Run several with different --name to see fan-out: every group gets every tick.
"""
import argparse
import asyncio
import time
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import DEFAULT_URL, STREAM, TOPIC, Window, unpack


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    consumer = await client.consumer_group(
        a.name, STREAM, TOPIC,
        polling_strategy=PollingStrategy.Last() if a.from_now else PollingStrategy.Next(),
        batch_length=1000,
        auto_commit=AutoCommit.Interval(timedelta(seconds=1)),
        poll_interval=timedelta(microseconds=100),
    )
    last, gaps, win, stop = {}, 0, Window(), asyncio.Event()

    async def on_tick(m):
        nonlocal gaps
        sym, seq, sent_ns, _, _ = unpack(m.payload())
        win.add((time.time_ns() - sent_ns) / 1000)
        prev = last.get(sym, seq - 1)
        if seq <= prev:      # re-read after a restart (at-least-once): skip the duplicate
            return
        if seq > prev + 1:   # a hole in this symbol's sequence
            gaps += 1
        last[sym] = seq
        if win.ready():
            r = win.flush()
            ok = "PASS" if r["p99"] < a.budget_us else "FAIL"
            print(f"[{a.name}] {r['n']:>7,}/s  p50 {r['p50']:7.0f}µs  p99 {r['p99']:7.0f}µs  "
                  f"p99.9 {r['p999']:7.0f}µs  max {r['max']:7.0f}µs  gaps {gaps}  {ok} (<{a.budget_us:,}µs)",
                  flush=True)

    await consumer.consume_messages(on_tick, stop)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--name", default="risk")
    p.add_argument("--budget-us", type=int, default=5000, help="p99 budget, microseconds")
    p.add_argument("--from-now", action="store_true", help="skip history, start at the live edge")
    asyncio.run(main(p.parse_args()))
