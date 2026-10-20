"""Evaluates alert rules on the stream itself, before anything is indexed.

Rule: error rate above 5% over the last 10 s for a service, with at least 50 requests.
Prints ingest lag (event emitted -> seen by the alerter) every second, and when the
incident fires, the time from the first failing request to the alert.
"""
import argparse
import asyncio
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import ALERTS, DEFAULT_URL, EVENTS, STREAM, Window, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    events = await client.consumer_group("alerting", STREAM, EVENTS, polling_strategy=PollingStrategy.Last(), batch_length=5000,
                                         auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(milliseconds=1))
    out = await client.producer(STREAM, ALERTS)
    win = defaultdict(deque)          # service -> (time, is_error) over the last 10 s
    firing, lag, incident_ns = set(), Window(), None

    async def on_event(m):
        nonlocal incident_ns
        e = load(m.payload())
        lag.add((time.time_ns() - e["ns"]) / 1e6)
        if e.get("incident_start"):
            incident_ns = e["ns"]
        now, s = time.monotonic(), e["svc"]
        q = win[s]
        q.append((now, e["status"] >= 500))
        while q and now - q[0][0] > 10:
            q.popleft()
        errors = sum(1 for _, bad in q if bad)
        if len(q) >= 50 and errors / len(q) > 0.05 and s not in firing:
            firing.add(s)
            await out.send_with_partitioning([SendMessage(msg(svc=s, rule="error_rate>5%/10s", rate=round(errors / len(q), 3)))],
                                             Partitioning.messages_key(s))
            took = f"{(time.time_ns() - incident_ns) / 1e9:.2f}s after the first failing request" if incident_ns else ""
            print(f"ALERT {s} error rate {errors / len(q):.0%} {took}", flush=True)
        elif s in firing and errors / max(1, len(q)) < 0.01:
            firing.discard(s)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = lag.flush()
            print(f"{r['n']:>8,} events/s  ingest-to-alerter p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
                  f"p99.9 {r['p999']:6.1f}ms  firing {len(firing)}", flush=True)

    async with out:
        await asyncio.gather(events.consume_messages(on_event, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
