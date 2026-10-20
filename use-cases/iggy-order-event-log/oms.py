"""Order management: accepts orders from clients, runs a pre-trade risk check, and writes each
order event to the log before telling the client it is accepted. The log is the order book's
write-ahead record, so the time to write it is time the trader waits.

Prints order-acknowledgement latency (order received -> written and acknowledged by the log),
which is the number a broker's customers feel. --persisted waits for disk as well as replicas.
"""
import argparse
import asyncio
import random
import time
from collections import defaultdict
from datetime import timedelta

from apache_iggy import DirectProducerConfig, Durability, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, ORDERS, STREAM, Window, msg

SYMBOLS = ["AAPL", "NVDA", "MSFT", "AMZN", "TSLA", "META", "SPY", "QQQ"]


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    log = await client.producer(STREAM, ORDERS, topic_partitions_count=a.partitions,
                                topic_durability=Durability.PERSISTED if a.persisted else Durability.REPLICATED,
                                mode=DirectProducerConfig(batch_length=1, linger_time=timedelta(microseconds=50)))
    seq, exposure, ack = defaultdict(int), defaultdict(float), Window()
    t0, due, n = time.monotonic(), time.monotonic(), 0

    async def submit():
        nonlocal n
        n += 1
        acct = f"acct{random.randrange(a.accounts)}"
        sym, qty, px = random.choice(SYMBOLS), random.randint(1, 200), round(random.uniform(50, 900), 2)
        received = time.time_ns()
        ok = exposure[acct] + qty * px < a.limit                     # pre-trade risk check
        if ok:
            exposure[acct] += qty * px
        seq[acct] += 1
        ev = msg(acct=acct, seq=seq[acct], order=f"o{n}", t="accepted" if ok else "rejected", sym=sym, qty=qty, px=px)
        await log.send_with_partitioning([SendMessage(ev)], Partitioning.messages_key(acct))
        ack.add((time.time_ns() - received) / 1e6)                  # written before the client hears back

    async with log:
        while True:
            t = time.monotonic() - t0
            rate = a.rate * (a.burst if a.open_at and a.open_at <= t < a.open_at + 60 else 1)
            await asyncio.gather(*(submit() for _ in range(max(1, int(rate / 1000)))))
            due += 0.001
            await asyncio.sleep(max(0.0, due - time.monotonic()))
            if ack.ready():
                r = ack.flush()
                ok = "PASS" if r["p99"] < a.budget_ms else "FAIL"
                print(f"{r['n']:>6} orders/s  order ack p50 {r['p50']:6.2f}ms  p99 {r['p99']:6.2f}ms  p99.9 {r['p999']:6.2f}ms  "
                      f"{'persisted' if a.persisted else 'replicated'}  {ok} (<{a.budget_ms}ms)", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--rate", type=int, default=2000, help="orders per second")
    ap.add_argument("--accounts", type=int, default=50000)
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--limit", type=float, default=250000, help="max open exposure per account")
    ap.add_argument("--persisted", action="store_true")
    ap.add_argument("--burst", type=float, default=1.0, help="multiplier for the first minute after --open-at")
    ap.add_argument("--open-at", type=float, default=0)
    ap.add_argument("--budget-ms", type=float, default=5)
    asyncio.run(main(ap.parse_args()))
