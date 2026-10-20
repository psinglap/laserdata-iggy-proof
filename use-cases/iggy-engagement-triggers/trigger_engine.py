"""Turns behaviour into actions, per user, as events arrive.

Two kinds of trigger:
  in-session  three views of one category within 60 s -> an offer while the user is still in the app.
              Measured from the third view to the trigger: this is the number users feel.
  timer       add to cart with no purchase within --cart-s seconds -> a reminder.
              Measured as lateness: how long after it was due the reminder actually fired.
Run several engines in the same group to share users; state per user lives in one engine
because events are keyed by user.
"""
import argparse
import asyncio
import heapq
import time
from collections import defaultdict, deque
from datetime import timedelta

from apache_iggy import AutoCommit, IggyClient, Partitioning, PollingStrategy, SendMessage

from common import DEFAULT_URL, EVENTS, STREAM, TRIGGERS, Window, load, msg


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    events = await client.consumer_group("triggers", STREAM, EVENTS, polling_strategy=PollingStrategy.Next(), batch_length=2000,
                                         auto_commit=AutoCommit.Interval(timedelta(seconds=1)), poll_interval=timedelta(milliseconds=1))
    out = await client.producer(STREAM, TRIGGERS, topic_partitions_count=16)
    views, carts, last_seq, timers = defaultdict(deque), {}, {}, []
    in_session, lateness, n = Window(), Window(), 0

    async def fire(user, kind, **d):
        nonlocal n
        n += 1
        await out.send_with_partitioning([SendMessage(msg(id=f"t{n}-{user}-{kind}", u=user, kind=kind, **d))], Partitioning.messages_key(user))

    async def on_event(m):
        e = load(m.payload())
        u = e["u"]
        if e["seq"] <= last_seq.get(u, 0):
            return                                    # redelivered after a restart: already handled
        last_seq[u] = e["seq"]
        now = time.monotonic()
        if e["e"] == "view":
            q = views[(u, e["cat"])]
            q.append(now)
            while q and now - q[0] > 60:
                q.popleft()
            if len(q) == 3:
                await fire(u, "offer", cat=e["cat"])
                in_session.add((time.time_ns() - e["ns"]) / 1e6)
        elif e["e"] == "add_to_cart":
            carts[u] = now + a.cart_s
            heapq.heappush(timers, (carts[u], u))
        elif e["e"] == "purchase":
            carts.pop(u, None)

    async def tick():
        while True:
            await asyncio.sleep(0.01)
            now = time.monotonic()
            while timers and timers[0][0] <= now:
                due, u = heapq.heappop(timers)
                if carts.get(u) == due:                 # still in the cart, not bought since
                    del carts[u]
                    await fire(u, "cart_reminder")
                    lateness.add((now - due) * 1000)

    async def report():
        while True:
            await asyncio.sleep(1)
            s, l = in_session.flush(), lateness.flush()
            ok = "PASS" if s["p99"] < a.budget_ms else "FAIL"
            print(f"in-session offers {s['n']:>5}/s  event-to-trigger p50 {s['p50']:6.1f}ms  p99 {s['p99']:6.1f}ms  "
                  f"| cart reminders {l['n']:>5}/s  late by p99 {l['p99']:6.1f}ms  {ok} (<{a.budget_ms}ms)", flush=True)

    async with out:
        await asyncio.gather(events.consume_messages(on_event, asyncio.Event()), tick(), report())


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--cart-s", type=float, default=30, help="reminder delay after add-to-cart without purchase")
    p.add_argument("--budget-ms", type=int, default=1000)
    asyncio.run(main(p.parse_args()))
