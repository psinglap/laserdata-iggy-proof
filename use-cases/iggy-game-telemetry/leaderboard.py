"""A live leaderboard: counts kills per player from the telemetry stream and publishes the
top 10 every 500 ms. Prints event-to-leaderboard latency: from the kill happening on the
game server to it counting on the board players see.
"""
import argparse
import asyncio
import time
from collections import Counter
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy, SendMessage

from common import DEFAULT_URL, LEADERBOARD, STREAM, TELEMETRY, Window, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    events = await client.consumer_group("leaderboard", STREAM, TELEMETRY, polling_strategy=PollingStrategy.Next(), batch_length=5000,
                                         auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(milliseconds=1))
    out = await client.producer(STREAM, LEADERBOARD)
    kills, pending, lat = Counter(), [], Window()

    async def on_event(m):
        e = load(m.payload())
        if e["e"] == "kill":
            kills[e["player"]] += 1
            pending.append(e["ns"])

    async def publish():
        async with out:
            while True:
                await asyncio.sleep(0.5)
                await out.send([SendMessage(msg(top=kills.most_common(10)))])
                now = time.time_ns()
                for ns in pending:
                    lat.add((now - ns) / 1e6)
                pending.clear()
                if lat.ready():
                    r = lat.flush()
                    print(f"leaderboard  kill-to-board p50 {r['p50']:6.0f}ms  p99 {r['p99']:6.0f}ms  (board refresh 500 ms)", flush=True)

    await asyncio.gather(events.consume_messages(on_event, asyncio.Event()), publish())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    asyncio.run(main(p.parse_args()))
