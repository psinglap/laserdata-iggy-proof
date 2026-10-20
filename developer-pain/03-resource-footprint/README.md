> **LaserData proof · Developer pain 03: Memory / CPU / JVM / GC**  
> Blog: [Running Apache Iggy small: one binary on one or two cores, at the edge, as a sidecar or on small nodes](https://claude.ai/artifact/NTugtAhsMsQAnXFCLforrB) · Doc: [Size an Apache Iggy deployment](https://claude.ai/artifact/16tgJNuUiuN6CtMt5yKiLq) · Posts: [ready-to-publish drafts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#resource)  
> Where this code goes: LaserData repo: a measurement kit anyone can run.  
> Status: not yet run against a live Apache Iggy server unless a checklist below says so. Nothing has been opened upstream.

# Apache Iggy footprint kit, from LaserData

Measures how much CPU and memory an Apache Iggy server uses when idle and at
10K, 100K and 1M messages per second, so you can size nodes from your own
numbers instead of ours.

Built by the LaserData team, which builds and runs Apache Iggy.

## What it measures

| Phase | Load | Length |
|---|---|---|
| idle | none, server just started | 2 min |
| 10000 | 10K msg/s, 256 B, 8 partitions, one consumer group | 5 min |
| 100000 | 100K msg/s | 5 min |
| 1000000 | 1M msg/s | 5 min |

Memory and CPU come from the server's own stats (`get_stats()`): process RSS and
CPU summed over cores, sampled every second. The first 30 s of each phase are dropped.

## Run it

```bash
pip install apache-iggy
IGGY_CPUS=2 IGGY_MEM=2g ./run.sh      # small node
IGGY_CPUS=8 IGGY_MEM=8g ./run.sh      # larger node
```

Each run writes `footprint-<N>cpu.csv` and a Markdown table of medians and p95s.

## Rules we follow when publishing

- The load generator runs on a separate machine (or disjoint CPUs) for the 100K and 1M steps.
  `loadgen.py` warns when it falls below 95% of the target rate; a run with that warning is not published.
- Record the image tag, host type, kernel, disk and the `IGGY_*` settings with every result.
- Publish every run, including the ones that look bad.

## Settings that change the footprint

| Setting | Env var | Effect |
|---|---|---|
| `sharding.cpu_allocation` | `IGGY_SHARDING_CPU_ALLOCATION` | Number of shards. Each shard is a thread on its own core with its own buffers. |
| `memory_pool.size` | `IGGY_MEMORY_POOL_SIZE` | Budget for pooled buffers, allocated on demand. Minimum 512 MiB. |
| topic `segment_size` | set at topic creation | Up to 1 GiB per segment on disk. |
| topic `preallocate_segments` | set at topic creation | Reserves a full segment of disk per partition up front. Off by default. |

Disclosure: I work at LaserData, which builds and runs Apache Iggy.
