"""Simulated screens: K WebSocket clients spread over gateways and matches.

Each prints nothing; together they report event-to-screen latency every second,
plus gaps (a seq skipped) and resumes (reconnects that continued without a gap).
--storm-at drops every connection at once and reconnects: the moment a gateway
restarts or a network blips during a final.
"""
import argparse
import asyncio
import json
import random
import time

from websockets.asyncio.client import connect

from common import Window

win, stats = Window(), {"gaps": 0, "resumes": 0, "dup": 0}
storm = asyncio.Event()


async def screen(gateways, match):
    last = 0
    while True:
        url = f"{random.choice(gateways)}/?match={match}&after={last}"
        try:
            async with connect(url, max_queue=None) as ws:
                if last:
                    stats["resumes"] += 1
                stop = asyncio.ensure_future(storm.wait())
                while True:
                    recv = asyncio.ensure_future(ws.recv())
                    done, _ = await asyncio.wait({recv, stop}, return_when=asyncio.FIRST_COMPLETED)
                    if stop in done:
                        recv.cancel()
                        break
                    ev = json.loads(recv.result())
                    win.add((time.time_ns() - ev["ns"]) / 1e6)
                    if ev["t"] == "reactions":       # live counts carry no sequence
                        continue
                    if ev["seq"] <= last:
                        stats["dup"] += 1
                        continue
                    if last and ev["seq"] > last + 1:
                        stats["gaps"] += 1
                    last = ev["seq"]
        except Exception:
            await asyncio.sleep(0.2)
        await asyncio.sleep(random.uniform(0, 1.0))   # jittered reconnect, as real apps do


async def report(budget):
    while True:
        await asyncio.sleep(1)
        r = win.flush()
        ok = "PASS" if r["p99"] < budget else "FAIL"
        print(f"{r['n']:>8,} events/s to screens  p50 {r['p50']:6.1f}ms  p99 {r['p99']:6.1f}ms  "
              f"p99.9 {r['p999']:6.1f}ms  gaps {stats['gaps']}  resumes {stats['resumes']}  {ok} (<{budget}ms)", flush=True)


async def main(a):
    gateways = [g.rstrip("/") for g in a.gateways.split(",")]
    tasks = [asyncio.ensure_future(screen(gateways, f"M{random.randrange(a.matches):03d}")) for _ in range(a.screens)]
    tasks.append(asyncio.ensure_future(report(a.budget_ms)))
    if a.storm_at:
        await asyncio.sleep(a.storm_at)
        print("--- storm: every screen disconnects and reconnects ---", flush=True)
        storm.set()
        await asyncio.sleep(0.5)
        storm.clear()
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--gateways", default="ws://localhost:8765")
    p.add_argument("--screens", type=int, default=2000)
    p.add_argument("--matches", type=int, default=50)
    p.add_argument("--budget-ms", type=int, default=100)
    p.add_argument("--storm-at", type=float, default=0)
    asyncio.run(main(p.parse_args()))
