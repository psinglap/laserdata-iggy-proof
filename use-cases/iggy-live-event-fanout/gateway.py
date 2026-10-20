"""Stateless WebSocket push gateway. Run as many as you like behind a load balancer.

Each gateway reads every event through its own consumer group, keeps the last few
thousand events per match in memory, and pushes them to the screens subscribed to
that match. A screen that reconnects sends ?match=M007&after=1234 and receives
everything after seq 1234, from this gateway or any other, so nothing is missed.
On restart a gateway rebuilds its buffers by replaying the last two minutes from Iggy.
"""
import argparse
import asyncio
import json
import os
import time
from collections import defaultdict, deque
from datetime import timedelta
from urllib.parse import parse_qs, urlparse

from apache_iggy import AutoCommit, IggyClient, PollingStrategy
from websockets.asyncio.server import serve

from common import DEFAULT_URL, EVENTS, STREAM

buffers = defaultdict(lambda: deque(maxlen=5000))   # match -> recent events (bytes)
screens = defaultdict(set)                           # match -> open sockets


async def screen(ws):
    q = parse_qs(urlparse(ws.request.path).query)
    match, after = q.get("match", ["M000"])[0], int(q.get("after", ["0"])[0])
    for raw in list(buffers[match]):                 # resume: everything after the last seq it saw
        if json.loads(raw)["seq"] > after:
            await ws.send(raw.decode())
    screens[match].add(ws)
    try:
        await ws.wait_closed()
    finally:
        screens[match].discard(ws)


async def pump(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    consumer = await client.consumer_group(
        f"gateway-{a.id}-{os.getpid()}", STREAM, EVENTS,
        polling_strategy=PollingStrategy.Timestamp(int((time.time() - a.replay) * 1_000_000)),
        batch_length=1000, auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(milliseconds=1))

    async def on_event(m):
        raw = m.payload()
        match = json.loads(raw)["m"]
        buffers[match].append(raw)
        dead = []
        for ws in screens[match]:
            if ws.state.name != "OPEN":
                dead.append(ws)
                continue
            asyncio.ensure_future(ws.send(raw.decode()))  # do not let one slow screen hold the others
        for ws in dead:
            screens[match].discard(ws)

    await consumer.consume_messages(on_event, asyncio.Event())


async def main(a):
    async with serve(screen, "0.0.0.0", a.port, max_size=2**16):
        print(f"gateway {a.id} on ws://localhost:{a.port}/?match=M000", flush=True)
        await pump(a)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--id", default="g1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--replay", type=int, default=120, help="seconds of history to rebuild on start")
    asyncio.run(main(p.parse_args()))
