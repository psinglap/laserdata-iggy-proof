# Benchmark plan: market-data fan-out

**Question it answers:** can a replicated Apache Iggy cluster deliver every tick to every desk inside a
5 ms end-to-end p99, at the load a tier-1 bank published, and does Kafka tuned for latency do the same
on the same machines?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Message size | 128 B | Top-of-book ticks are 40–200 B. 128 B keeps the test about per-message cost, not bandwidth. |
| Peak rate | 100K → 500K → 1M → 1.6M ticks/s | 1.6M msg/s is the sustained load in Confluent's published tier-1 bank design. |
| Partitions | 16, keyed by symbol | Per-symbol order must hold, so a symbol never spans partitions. |
| Fan-out | 4 consumer groups (risk, P&L, pricing, surveillance) | Market data is read many times. Fan-out is where brokers that copy per subscriber fall behind. |
| Durability | Iggy `replicated` (3 nodes) vs Kafka `acks=all`, RF 3, `min.insync.replicas=2`, idempotent | Banks do not relax acks for speed. Neither do we. |
| Kafka tuning | `linger.ms=0`, `fetch.max.wait.ms=0`, no compression | Kafka's best latency settings, so the comparison is fair. |
| Metric | End-to-end p50 / p99 / p99.9 / max, per second | Averages hide the ticks that arrive late. At 1.6M/s, p99 is 16,000 ticks every second. |
| Burst | 10× for 60 s (market open) | Market open is when budgets are blown and on-call gets paged. |
| Duration | 5 min warm-up, 30 min per step, then one 24 h soak at 1M/s | The bank ran 10–60 min tests plus 24 h soaks. |
| Runs | 3 per step, report the median run | One lucky run is not a result. |
| Persisted run | Repeat the 1M/s step with Iggy `persisted` and Kafka `flush.messages=1` | `replicated` acks after a quorum holds the write (like `acks=all`); `persisted` also waits for stable storage. Buyers ask for both. |
| Failover | Kill the partition leader 15 min into the 1M/s step | Report time until desks receive again and p99 for the following minute. Iggy 0.9 uses Viewstamped Replication; 3 nodes tolerate 1 failure. |

## Hardware (same for both systems)

- 3 × `c6id.4xlarge` (16 vCPU, 32 GB, local NVMe) brokers in one AZ, plus 1 × `c7i.4xlarge` load generator.
- Same kernel and XFS, published host tuning (`iggy-bench` README: swap, huge pages, writeback).
- Software versions, every config file and raw outputs go in this folder with the results.

## Pass line

End-to-end p99 under 5 ms at 1.6M ticks/s with replication, and zero sequence gaps at every desk.

## What we publish

1. `p99-vs-rate.png` from `compare.py` (one line per system, the 5 ms budget drawn on it).
2. A per-second p99 trace through the market-open burst.
3. Broker CPU and node count at each step (cost).
4. All configs, versions and raw outputs. If Kafka wins a step, that row is published too.
