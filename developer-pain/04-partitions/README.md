> **LaserData proof · Developer pain 04: Partition limits & hot partitions**  
> Blog: [How partitions work in Apache Iggy, and how many you actually need](https://claude.ai/artifact/Mp6DiKAoiMLTroUWh8VDuV) · Doc: [Plan streams, topics and partitions in Apache Iggy](https://claude.ai/artifact/XnKggTfVSJN8v1krnFPqRH) · Posts: [ready-to-publish drafts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#partitions)  
> Where this code goes: LaserData repo: a scale-test kit anyone can run.  
> Status: not yet run against a live Apache Iggy server unless a checklist below says so. Nothing has been opened upstream.

# Apache Iggy partition scale kit, from LaserData

Answers two questions with measurements from your own node:

1. What limits does Apache Iggy put on partitions? (`limits.py`)
2. What does each extra partition cost in memory, CPU and latency? (`scale.py`)

Built by the LaserData team, which builds and runs Apache Iggy.

## The limits, from the 0.9 source

| Limit | Value | Where |
|---|---|---|
| Partitions per create call (`create_topic`, `create_partitions`) | 1,000 | `MAX_PARTITIONS_COUNT` in `core/common/src/lib.rs`, `MAX_PARTITIONS_PER_REQUEST` in `core/binary_protocol` |
| Partition ids per topic | 1,000,000 | `MAX_PARTITIONS` in `core/binary_protocol/src/namespace.rs` |
| Topics per stream | 4,096 | `MAX_TOPICS`, same file |
| Streams | 1,048,576 | `MAX_STREAMS`, same file |

The 1,000 figure is per call, not per topic: a topic grows past it by calling
`create_partitions` again. `limits.py` checks both behaviours on a live server.

## Run it

```bash
pip install apache-iggy
docker compose up -d            # or point --url at your own node
python limits.py
python scale.py --steps 100,1000,5000,10000,25000,50000
```

`scale.py` writes `partitions.csv` with, for each step: time to create the
partitions, server RSS and CPU idle and under a light round-robin load, and p99
produce latency for a 64-byte message. Test topics use 16 MiB segments so disk
isn't the first limit; pass `--segment-mib 1024` to test the default.

## What to expect to matter

- **Single node:** each partition has its own log files and write-ahead log (budget `partition.wal_bytes_max`, 256 MiB by default).
- **Cluster:** each partition is also its own replication group and keeps a repair ring of recent writes, up to `partition.evicted_ring_bytes_max` (64 MiB by default) on busy partitions, with no node-wide cap. Run `scale.py` against a 3-node cluster too; that is where per-partition cost shows.

## Publishing results

Record the image tag, host, kernel, disk, core count and every `IGGY_*` setting.
Publish every run, including the ones that look bad.

Disclosure: I work at LaserData, which builds and runs Apache Iggy.
