"""Partition scale test for one Apache Iggy node.

Adds partitions in steps and, at each step, records:
  - how long the create calls took,
  - server memory (RSS) and CPU from the server's own stats, idle and under light load,
  - p99 produce latency for a small message sent round-robin across every partition.
Writes one CSV row per step. Steps default to 100, 1K, 5K, 10K, 25K and 50K partitions.
"""
import argparse, asyncio, csv, time, uuid

from apache_iggy import IggyClient, Partitioning, SendMessage


def p99(xs):
    xs = sorted(xs)
    return xs[int(0.99 * (len(xs) - 1))] if xs else float("nan")


async def stats(c):
    s = await c.get_stats()
    return round(s.memory_usage / 2**20, 1), round(s.cpu_usage, 1), s.partitions_count


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="iggy+tcp://iggy:iggy@127.0.0.1:8090")
    p.add_argument("--steps", default="100,1000,5000,10000,25000,50000")
    p.add_argument("--per-topic", type=int, default=1000, help="partitions per topic (max 1,000 per create call)")
    p.add_argument("--segment-mib", type=int, default=16, help="segment size for test topics")
    p.add_argument("--sends", type=int, default=5000, help="messages sent per step for the latency sample")
    p.add_argument("--out", default="partitions.csv")
    args = p.parse_args()

    c = IggyClient.from_connection_string(args.url)
    await c.connect()
    stream = f"scale-{uuid.uuid4().hex[:6]}"
    await c.create_stream(stream)
    topics, have = [], 0

    with open(args.out, "a", newline="") as f:
        w = csv.writer(f)
        if f.tell() == 0:
            w.writerow(["target", "server_partitions", "create_s", "idle_rss_mib", "idle_cpu_pct",
                        "load_rss_mib", "load_cpu_pct", "produce_p99_ms"])
        for target in [int(s) for s in args.steps.split(",")]:
            t0 = time.perf_counter()
            while have < target:
                n = min(args.per_topic, target - have)
                name = f"t{len(topics)}"
                await c.create_topic(stream, name, partitions_count=n, segment_size=args.segment_mib * 2**20)
                topics.append((name, n))
                have += n
            create_s = time.perf_counter() - t0
            await asyncio.sleep(30)                      # let the server settle
            idle_rss, idle_cpu, server_parts = await stats(c)

            lat = []
            for i in range(args.sends):
                name, n = topics[i % len(topics)]
                part = i // len(topics) % n
                t = time.perf_counter()
                await c.send_messages(stream, name, Partitioning.partition_id(part), [SendMessage(b"x" * 64)])
                lat.append((time.perf_counter() - t) * 1000)
            load_rss, load_cpu, _ = await stats(c)

            row = [target, server_parts, round(create_s, 1), idle_rss, idle_cpu, load_rss, load_cpu, round(p99(lat), 2)]
            print(row)
            w.writerow(row)
            f.flush()


if __name__ == "__main__":
    asyncio.run(main())
