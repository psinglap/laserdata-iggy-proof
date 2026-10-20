"""Live-ops economy watch: flags any player earning far more currency than normal within
10 seconds, the signature of a duplication exploit. Prints time from the first suspicious
gain to the flag, so live-ops knows how fast an exploit is caught.
"""
import argparse
import asyncio
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, FLAGS, STREAM, TELEMETRY, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    events = await client.consumer_group("economy", STREAM, TELEMETRY, polling_strategy=PollingStrategy.Next(), batch_length=5000,
                                         auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(milliseconds=1))
    out = await client.producer(STREAM, FLAGS)
    gains, flagged = defaultdict(deque), set()

    async def on_event(m):
        e = load(m.payload())
        if e["e"] != "currency" or e["player"] in flagged:
            return
        now, q = time.monotonic(), gains[e["player"]]
        q.append((now, e["amount"], e["ns"]))
        while q and now - q[0][0] > 10:
            q.popleft()
        if sum(x[1] for x in q) > a.limit:
            flagged.add(e["player"])
            first_ns = q[0][2]
            await out.send_with_partitioning([SendMessage(msg(player=e["player"], reason="currency_spike"))],
                                             Partitioning.messages_key(e["player"]))
            print(f"FLAG {e['player']} earned {sum(x[1] for x in q):,} in 10 s; caught "
                  f"{(time.time_ns() - first_ns) / 1e6:.0f} ms after the first suspicious gain", flush=True)

    async with out:
        await events.consume_messages(on_event, asyncio.Event())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--limit", type=int, default=2000, help="currency per player per 10 s before flagging")
    asyncio.run(main(p.parse_args()))
