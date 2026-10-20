"""Replays the last N minutes of authorizations through candidate thresholds, so a rule
change can be tested on real traffic before it ships. Reads by timestamp, at full speed.
"""
import argparse
import asyncio
import os
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, PollingStrategy

from common import AUTHS, DEFAULT_URL, STREAM, load


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    replay = await client.consumer_group(f"backtest-{os.getpid()}", STREAM, AUTHS,
                                         polling_strategy=PollingStrategy.Timestamp(int((time.time() - a.minutes * 60) * 1e6)),
                                         batch_length=10000, auto_commit=AutoCommit.Disabled(), poll_interval=timedelta(milliseconds=1))
    seen, idle, stop, t0 = defaultdict(deque), [time.monotonic()], asyncio.Event(), time.monotonic()
    res = {th: {"blocked": 0, "attack_caught": 0, "attack_total": 0, "good_blocked": 0} for th in a.device_thresholds}
    n = 0

    async def on_auth(m):
        nonlocal n
        x = load(m.payload())
        n += 1
        idle[0] = time.monotonic()
        t = x["ns"] / 1e9
        q = seen[x["device"]]
        q.append(t)
        while q and t - q[0] > 60:
            q.popleft()
        for th, r in res.items():
            hit = len(q) > th
            r["blocked"] += hit
            if x.get("attack"):
                r["attack_total"] += 1
                r["attack_caught"] += hit
            elif hit:
                r["good_blocked"] += 1

    async def until_idle():
        while time.monotonic() - idle[0] < 2:
            await asyncio.sleep(0.5)
        stop.set()

    await asyncio.gather(replay.consume_messages(on_auth, stop), until_idle())
    print(f"replayed {n:,} authorizations from the last {a.minutes} min in {time.monotonic() - t0 - 2:.1f}s")
    for th, r in res.items():
        print(f"device_1m > {th:>3}: attack caught {r['attack_caught']}/{r['attack_total']}  good payments blocked {r['good_blocked']}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--minutes", type=int, default=10)
    p.add_argument("--device-thresholds", type=int, nargs="+", default=[5, 10, 20, 40])
    asyncio.run(main(p.parse_args()))
