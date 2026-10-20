"""Callers on the phone: each call says something, waits for the agent's answer, then says
the next thing. Prints turn latency (caller stops speaking -> agent's reply ready) and the
orchestration overhead: turn latency minus the model and tool time the agent reported.
That overhead is what the event bus and the services around it cost per turn.
"""
import argparse
import asyncio
import random
import time
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, SESSION, STREAM, Window, bus, load, msg

LINES = ["where is my order", "can I change the delivery address", "what is my balance", "cancel my last payment"]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    out = await bus(client)
    replies = await client.consumer_group("callers", STREAM, SESSION, polling_strategy=PollingStrategy.Last(), batch_length=1000,
                                          auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(microseconds=200))
    waiting, turn, overhead, seen = {}, Window(), Window(), set()

    async def say(call, n):
        waiting[(call, n)] = time.time_ns()
        await out.send_with_partitioning([SendMessage(msg(call=call, turn=n, t="user_said", text=random.choice(LINES)))],
                                         Partitioning.messages_key(call))

    async def on_event(m):
        e = load(m.payload())
        if e["t"] != "agent_said" or (e["call"], e["turn"]) in seen:
            return                                        # only replies; skip a reply redelivered after a restart
        seen.add((e["call"], e["turn"]))
        start = waiting.pop((e["call"], e["turn"]), None)
        if start is None:
            return
        ms = (time.time_ns() - start) / 1e6
        turn.add(ms)
        overhead.add(ms - e["work_ms"])
        if e["turn"] < a.turns:
            asyncio.get_running_loop().call_later(random.uniform(0.5, 2.0),
                                                   lambda: asyncio.ensure_future(say(e["call"], e["turn"] + 1)))

    async def start_calls():
        for i in range(a.calls):
            await say(f"call-{i}", 1)
            await asyncio.sleep(a.ramp / a.calls)

    async def report():
        while True:
            await asyncio.sleep(1)
            t, o = turn.flush(), overhead.flush()
            ok = "PASS" if t["p99"] < a.budget_ms else "FAIL"
            print(f"{t['n']:>5} turns/s  turn p50 {t['p50']:6.0f}ms  p99 {t['p99']:6.0f}ms  |  orchestration overhead "
                  f"p50 {o['p50']:5.1f}ms  p99 {o['p99']:5.1f}ms  {ok} (<{a.budget_ms}ms)", flush=True)

    async with out:
        await asyncio.gather(replies.consume_messages(on_event, asyncio.Event()), start_calls(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--calls", type=int, default=500)
    p.add_argument("--turns", type=int, default=20)
    p.add_argument("--ramp", type=float, default=10, help="seconds to start all calls")
    p.add_argument("--budget-ms", type=int, default=800)
    asyncio.run(main(p.parse_args()))
