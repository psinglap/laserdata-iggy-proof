"""Writes the same events to ClickHouse in batches and measures event-to-queryable time.

In production, Iggy's ClickHouse sink connector does this without code; this file shows the
moving parts and the number that matters: how long until an engineer can query an event.
Uses ClickHouse's HTTP interface (no extra packages). Set --clickhouse '' to write files instead.
"""
import argparse
import asyncio
import json
import time
import urllib.request
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import DEFAULT_URL, EVENTS, STREAM, Window, load

DDL = ("CREATE TABLE IF NOT EXISTS events (ns UInt64, svc String, level String, status UInt16, ms Float32, msg String) "
       "ENGINE = MergeTree ORDER BY (svc, ns)")


def post(url, body: bytes):
    urllib.request.urlopen(urllib.request.Request(url, data=body, method="POST"), timeout=10).read()


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    events = await client.consumer_group("clickhouse-sink", STREAM, EVENTS, polling_strategy=PollingStrategy.Next(), batch_length=10000,
                                         auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(milliseconds=5))
    if a.clickhouse:
        post(a.clickhouse, DDL.encode())
    batch, fresh = [], Window()

    async def on_event(m):
        batch.append(load(m.payload()))

    async def flush():
        while True:
            await asyncio.sleep(a.flush_ms / 1000)
            if not batch:
                continue
            rows, batch[:] = list(batch), []
            body = "\n".join(json.dumps({k: r.get(k) for k in ("ns", "svc", "level", "status", "ms", "msg")}) for r in rows).encode()
            if a.clickhouse:
                await asyncio.to_thread(post, a.clickhouse + "?query=INSERT%20INTO%20events%20FORMAT%20JSONEachRow", body)
            else:
                with open(f"events-{int(time.time() // 60)}.jsonl", "ab") as f:
                    f.write(body + b"\n")
            now = time.time_ns()
            for r in rows[:: max(1, len(rows) // 1000)]:
                fresh.add((now - r["ns"]) / 1e6)

    async def report():
        while True:
            await asyncio.sleep(1)
            r = fresh.flush()
            print(f"sink  event-to-queryable p50 {r['p50']:7.1f}ms  p99 {r['p99']:7.1f}ms  (flush every {a.flush_ms} ms)", flush=True)

    await asyncio.gather(events.consume_messages(on_event, asyncio.Event()), flush(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--clickhouse", default="http://localhost:8123/")
    p.add_argument("--flush-ms", type=int, default=500)
    asyncio.run(main(p.parse_args()))
