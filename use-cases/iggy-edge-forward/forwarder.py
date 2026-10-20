"""Store and forward: reads the gateway's local Iggy and sends each reading to the cloud Iggy,
keyed by device. A reading's offset is committed on the gateway only after the cloud has
acknowledged it, so an outage leaves the backlog safely on the gateway's disk, and the
forwarder catches up in order when the link returns. Prints forwarded/s and how far behind
real time the cloud is.
"""
import argparse
import asyncio
import time
from datetime import timedelta

from apache_iggy import AutoCommit, AutoCommitAfter, Durability, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import CLOUD_URL, EDGE_URL, READINGS, STREAM, Window, load


async def main(a):
    edge, cloud = IggyClient.from_connection_string(a.edge), IggyClient.from_connection_string(a.cloud)
    await edge.connect()
    await cloud.connect()
    out = await cloud.producer(STREAM, READINGS, topic_partitions_count=16, topic_durability=Durability.REPLICATED)
    local = await edge.consumer_group("forwarder", STREAM, READINGS, polling_strategy=PollingStrategy.Next(), batch_length=a.batch,
                                      auto_commit=AutoCommit.After(AutoCommitAfter.ConsumingEachMessage()),
                                      poll_interval=timedelta(milliseconds=1))
    behind, retries = Window(), 0

    async def on_reading(m):
        nonlocal retries
        r = load(m.payload())
        wait = 0.2
        while True:                                   # hold this reading until the cloud has it
            try:
                await out.send_with_partitioning([SendMessage(m.payload())], Partitioning.messages_key(r["dev"]))
                break
            except Exception:
                retries += 1
                await asyncio.sleep(wait)
                wait = min(wait * 2, 5.0)
        behind.add((time.time_ns() - r["ns"]) / 1e9)

    async def report():
        while True:
            await asyncio.sleep(1)
            b = behind.flush()
            state = "catching up" if b["max"] > 2 else "live"
            print(f"forwarded {b['n']:>6}/s  cloud is behind by p50 {b['p50']:7.2f}s  max {b['max']:7.2f}s  "
                  f"send retries {retries}  [{state}]", flush=True)

    async with out:
        await asyncio.gather(local.consume_messages(on_reading, asyncio.Event()), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--edge", default=EDGE_URL)
    p.add_argument("--cloud", default=CLOUD_URL)
    p.add_argument("--batch", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
