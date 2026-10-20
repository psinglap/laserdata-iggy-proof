"""Delivers triggers as push, email or in-app messages, exactly once per trigger id.

Triggers can be redelivered after a restart (at-least-once), so the sender remembers the
ids it has sent recently and skips repeats. Prints trigger-to-send latency and how many
repeats it suppressed.
"""
import argparse
import asyncio
import time
from collections import OrderedDict
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import DEFAULT_URL, STREAM, TRIGGERS, Window, load


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    triggers = await client.consumer_group("sender", STREAM, TRIGGERS, polling_strategy=PollingStrategy.Next(), batch_length=1000,
                                           auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(milliseconds=1))
    sent, lat, dup = OrderedDict(), Window(), 0

    async def on_trigger(m):
        nonlocal dup
        t = load(m.payload())
        if t["id"] in sent:
            dup += 1
            return
        sent[t["id"]] = True
        if len(sent) > 1_000_000:
            sent.popitem(last=False)
        lat.add((time.time_ns() - t["ns"]) / 1e6)       # here a real sender calls APNs, FCM or an email API

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lat.flush()
            print(f"sent {r['n']:>5}/s  trigger-to-send p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  repeats suppressed {dup}", flush=True)

    await asyncio.gather(triggers.consume_messages(on_trigger, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
