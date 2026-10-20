"""A game server running matches at 128 ticks a second (7.8 ms per tick).

Each tick emits gameplay events (damage, kills, pickups, currency) through Iggy's background
producer: send() only puts the event in a buffer and returns, so the game loop never waits
for the network or the broker. If the buffer is ever full, the event is dropped and counted
rather than stalling the tick. Prints, every second: tick overruns, the time the loop spent
handing events off (p99, microseconds), and events dropped.
--tournament-at multiplies the number of live matches; --exploit-at gives one player a
currency duplication bug, which the economy watcher should catch.
"""
import argparse
import asyncio
import random
import time
from datetime import timedelta

from apache_iggy import BackgroundProducerConfig, BackpressureMode, Durability, IggyClient, Partitioning, SendMessage

from common import DEFAULT_URL, STREAM, TELEMETRY, Window, msg

TICK = 1 / 128


async def main(a):
    client = IggyClient.from_connection_string(a.url)
    await client.connect()
    p = await client.producer(STREAM, TELEMETRY, topic_partitions_count=a.partitions, topic_durability=Durability.REPLICATED,
                              mode=BackgroundProducerConfig(linger_time=timedelta(milliseconds=5), batch_length=2000,
                                                            failure_mode=BackpressureMode.fail_immediately()))
    handoff, overruns, dropped, n_tick = Window(), 0, 0, 0
    t0, nxt = time.monotonic(), time.monotonic()
    async with p:
        while True:
            start = time.perf_counter()
            t = time.monotonic() - t0
            live = a.matches * (a.tournament if a.tournament_at and a.tournament_at <= t < a.tournament_at + a.tournament_for else 1)
            exploit = a.exploit_at and t >= a.exploit_at
            by_match = {}
            for _ in range(max(1, int(live * a.events_per_match_tick))):
                mt = f"m{random.randrange(int(live))}"
                pl = f"{mt}-p{random.randrange(a.players)}"
                kind = random.choices(["damage", "kill", "pickup", "currency"], [10, 1, 3, 2])[0]
                ev = dict(match=mt, player=pl, e=kind, tick=n_tick)
                if kind == "currency":
                    ev["amount"] = random.randint(1, 20)
                by_match.setdefault(mt, []).append(SendMessage(msg(**ev)))
            if exploit:
                by_match.setdefault("m0", []).append(SendMessage(msg(match="m0", player="m0-p7", e="currency", amount=5000, tick=n_tick)))
            h0 = time.perf_counter()
            for mt, ms in by_match.items():
                try:
                    await p.send_with_partitioning(ms, Partitioning.messages_key(mt))
                except RuntimeError:
                    dropped += len(ms)                    # buffer full: drop, never stall the tick
            handoff.add((time.perf_counter() - h0) * 1e6)
            if time.perf_counter() - start > TICK:
                overruns += 1
            n_tick += 1
            nxt += TICK
            await asyncio.sleep(max(0.0, nxt - time.monotonic()))
            if handoff.ready():
                r = handoff.flush()
                print(f"{int(live):>4} matches  ticks/s {r['n']:>3}  overruns {overruns}  "
                      f"event handoff per tick p50 {r['p50']:6.0f}µs  p99 {r['p99']:6.0f}µs  dropped {dropped}", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--matches", type=int, default=50)
    ap.add_argument("--players", type=int, default=10, help="players per match")
    ap.add_argument("--events-per-match-tick", type=float, default=0.5)
    ap.add_argument("--partitions", type=int, default=16)
    ap.add_argument("--tournament", type=float, default=1.0, help="multiplier for live matches")
    ap.add_argument("--tournament-at", type=float, default=0)
    ap.add_argument("--tournament-for", type=float, default=120)
    ap.add_argument("--exploit-at", type=float, default=0)
    asyncio.run(main(ap.parse_args()))
