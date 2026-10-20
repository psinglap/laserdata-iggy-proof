"""Replays one call's complete history from the log: every utterance, tool call, result and
answer, in order, with timings. This is what evals, QA and compliance reviews read.
"""
import argparse
import asyncio
import os
import time
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import DEFAULT_URL, SESSION, STREAM, load


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    replay = await client.consumer_group(f"audit-{os.getpid()}", STREAM, SESSION,
                                         polling_strategy=PollingStrategy.Timestamp(int((time.time() - a.since) * 1e6)),
                                         batch_length=5000, auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(milliseconds=1))
    first, stop, idle = None, asyncio.Event(), [time.monotonic()]

    async def on_event(m):
        nonlocal first
        e = load(m.payload())
        idle[0] = time.monotonic()
        if e["call"] != a.call:
            return
        first = first or e["ns"]
        detail = e.get("text") or e.get("tool") or ""
        print(f"+{(e['ns'] - first) / 1e6:8.1f} ms  turn {e['turn']:>2}  {e['t']:<12} {detail}")

    async def until_idle():
        while time.monotonic() - idle[0] < 2:
            await asyncio.sleep(0.5)
        stop.set()

    await asyncio.gather(replay.consume_messages(on_event, stop), until_idle())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--call", default="call-7")
    p.add_argument("--since", type=int, default=600, help="seconds of history to scan")
    asyncio.run(main(p.parse_args()))
