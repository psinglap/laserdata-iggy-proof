"""Tool workers: pick up tool calls from the bus, do the work, post the result.

Any number of workers share the tool calls through one consumer group. --tool-ms simulates
an API or database call.
"""
import argparse
import asyncio
import random
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, SESSION, STREAM, bus, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    out = await bus(client)
    calls = await client.consumer_group("tools", STREAM, SESSION, polling_strategy=PollingStrategy.Last(), batch_length=1000,
                                        auto_commit=AutoCommit.Interval(timedelta(milliseconds=500)), poll_interval=timedelta(microseconds=200))

    async def handle(e):
        if e["t"] != "tool_call":
            return
        took = random.uniform(0.5, 1.5) * a.tool_ms
        await asyncio.sleep(took / 1000)
        await out.send_with_partitioning([SendMessage(msg(call=e["call"], turn=e["turn"], t="tool_result", ok=True, tool_ms=took))],
                                         Partitioning.messages_key(e["call"]))

    async def on_event(m):
        asyncio.ensure_future(handle(load(m.payload())))   # model and tool time must not block other calls

    async with out:
        await calls.consume_messages(on_event, asyncio.Event())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--tool-ms", type=float, default=40)
    asyncio.run(main(p.parse_args()))
