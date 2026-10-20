"""Zero-loss check for a rolling upgrade.

Writes numbered messages for DURATION_SECONDS, retrying through node restarts,
and remembers which sends were acknowledged. Then reads the whole partition
back and checks that every acknowledged number is there.

A send that fails without an acknowledgement may or may not have been stored,
so it can show up as a duplicate when retried. That is allowed (at-least-once).
An acknowledged message that is missing is a failure.
"""
import asyncio
import json
import os
import time
from urllib.parse import quote

from apache_iggy import (Consumer, Durability, IggyClient, Partitioning,
                         PollingStrategy, SendMessage)

SEED = os.environ["IGGY_SEED"]
USER, PASSWORD = os.environ["IGGY_USERNAME"], os.environ["IGGY_PASSWORD"]
DURATION = int(os.environ.get("DURATION_SECONDS", "900"))
STREAM, TOPIC = f"loss-check-{int(time.time())}", "seq"


async def connect() -> IggyClient:
    client = IggyClient.from_connection_string(
        f"iggy://{quote(USER, safe='')}:{quote(PASSWORD, safe='')}@{SEED}")
    await client.connect()
    return client


async def main() -> None:
    client = await connect()
    await client.create_stream(STREAM)
    # One partition keeps the read-back simple; persisted means an ack waits for
    # a stable-storage copy on a quorum.
    await client.create_topic(STREAM, TOPIC, partitions_count=1,
                              durability=Durability.PERSISTED)

    acked, attempts, errors, seq = set(), 0, 0, 0
    deadline = time.monotonic() + DURATION
    while time.monotonic() < deadline:
        seq += 1
        while True:
            attempts += 1
            try:
                await client.send_messages(STREAM, TOPIC, Partitioning.partition_id(0),
                                           [SendMessage(str(seq))])
                acked.add(seq)
                break
            except RuntimeError:
                errors += 1
                await asyncio.sleep(0.5)       # the SDK reconnects; retry the same number
        if seq % 1000 == 0:
            print(json.dumps({"progress": seq, "send_errors": errors}), flush=True)
        await asyncio.sleep(0.01)              # about 100 messages per second

    found, offset = {}, 0
    consumer = Consumer.Single("loss-check")
    while True:
        batch = await client.poll_messages(STREAM, TOPIC, consumer=consumer,
                                           polling_strategy=PollingStrategy.Offset(offset),
                                           count=1000, auto_commit=False, partition_id=0)
        if not batch:
            break
        for m in batch:
            n = int(m.payload().decode())
            found[n] = found.get(n, 0) + 1
        offset = batch[-1].offset() + 1

    missing = sorted(acked - found.keys())
    result = {"acknowledged": len(acked), "read_back": sum(found.values()),
              "missing_acknowledged": len(missing), "first_missing": missing[:10],
              "duplicates": sum(c - 1 for c in found.values() if c > 1),
              "send_errors_retried": errors, "send_attempts": attempts,
              "verdict": "PASS" if not missing else "FAIL"}
    print(json.dumps(result), flush=True)
    raise SystemExit(0 if not missing else 1)


asyncio.run(main())
