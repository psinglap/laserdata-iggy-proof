"""The agent: hears the caller, decides, calls a tool through the bus, and answers.

Each turn crosses the log four times: user_said -> agent, tool_call -> tool worker,
tool_result -> agent, agent_said -> caller. --llm-ms simulates the model's time to first
token. Run several agents in the same group; calls are keyed, so a call's turns stay
with one agent. After a crash, the agent resumes from its committed offset and the
session log tells it exactly where each call was.
"""
import argparse
import asyncio
import random
import time
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, SESSION, STREAM, bus, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    out = await bus(client)
    events = await client.consumer_group("agents", STREAM, SESSION, polling_strategy=PollingStrategy.Last(), batch_length=1000,
                                         auto_commit=AutoCommit.Interval(timedelta(milliseconds=500)), poll_interval=timedelta(microseconds=200))
    started = {}   # (call, turn) -> model ms spent so far

    async def emit(e):
        await out.send_with_partitioning([SendMessage(msg(**e))], Partitioning.messages_key(e["call"]))

    async def handle(e):
        key = (e["call"], e["turn"])
        if e["t"] == "user_said":
            llm = random.uniform(0.6, 1.4) * a.llm_ms
            await asyncio.sleep(llm / 1000)                       # model decides to call a tool
            started[key] = llm
            await emit({"call": e["call"], "turn": e["turn"], "t": "tool_call", "tool": "lookup_order"})
        elif e["t"] == "tool_result" and key in started:
            llm = random.uniform(0.6, 1.4) * a.llm_ms
            await asyncio.sleep(llm / 1000)                       # model writes the answer
            work = started.pop(key) + llm + e["tool_ms"]
            await emit({"call": e["call"], "turn": e["turn"], "t": "agent_said", "text": "Your order arrives tomorrow.", "work_ms": work})

    async def on_event(m):
        asyncio.ensure_future(handle(load(m.payload())))   # model and tool time must not block other calls

    async with out:
        await events.consume_messages(on_event, asyncio.Event())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--llm-ms", type=float, default=150, help="simulated model time per step")
    asyncio.run(main(p.parse_args()))
