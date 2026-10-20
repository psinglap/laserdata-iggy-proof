"""Samples the server's own CPU and memory once a second into a CSV.

Uses the server stats endpoint through the SDK, so it works the same on bare
metal, in Docker and on Kubernetes. Columns: unix time, phase label, server RSS
in MiB, server CPU percent summed over cores, partitions, connected clients.
"""
import argparse, asyncio, csv, sys, time

from apache_iggy import IggyClient


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="iggy+tcp://iggy:iggy@127.0.0.1:8090")
    p.add_argument("--phase", required=True)
    p.add_argument("--seconds", type=int, required=True)
    p.add_argument("--out", default="footprint.csv")
    args = p.parse_args()
    client = IggyClient.from_connection_string(args.url)
    await client.connect()
    with open(args.out, "a", newline="") as f:
        w = csv.writer(f)
        if f.tell() == 0:
            w.writerow(["ts", "phase", "rss_mib", "cpu_pct", "partitions", "clients"])
        end = time.time() + args.seconds
        while time.time() < end:
            s = await client.get_stats()
            w.writerow([int(time.time()), args.phase, round(s.memory_usage / 2**20, 1),
                        round(s.cpu_usage, 1), s.partitions_count, s.clients_count])
            f.flush()
            await asyncio.sleep(1)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
