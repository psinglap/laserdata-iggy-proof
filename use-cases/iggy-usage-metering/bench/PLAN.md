# Benchmark plan: usage metering and quotas

**Question it answers:** at a million usage events a second, does every event reach the meter within
20 ms at p99, so quotas cut off on time and minute totals close on schedule, with persisted writes
as billing teams expect, on Apache Iggy and on Kafka tuned for latency, on the same machines?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Event size | 200 B | Event id, tenant, units, kind and timestamp. |
| Rate | 100K → 500K → 1M → 2M events/s | API and inference platforms meter every request. |
| Partitions | 32, keyed by tenant | A tenant's usage stays in order and with one meter. |
| Readers | 3 consumer groups | Meter, quota enforcement and analytics. |
| Durability | Iggy persisted (fsync before ack) · Kafka acks=all, RF 3, flush.messages=1 | Usage is money; billing teams usually require it on disk. |
| Replicated run | Same steps with Iggy replicated / Kafka default flush | Shows what persisting costs, for teams that accept replication alone. |
| Burst | One tenant 10× for 10 min | A customer's batch job or runaway agent. |
| Failover | Kill a leader at minute 15 | Report time until the meter receives again and overage during the gap. |
| Duration | 30 min per step + 24 h soak | Billing closes daily. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Hardware

3 × `c6id.4xlarge` brokers (local NVMe), 2 × `c7i.4xlarge` load generators. Same kernel and filesystem.

## Pass lines

1. Event-to-meter p99 under 20 ms at 1M events/s with 3 readers, persisted.
2. In the kit, reconcile.py reports 0 mismatches after the 24 h soak, including the failover.

## What we publish

p99 vs event rate for persisted and replicated runs (one line per system each), overage during the
burst and failover, reconciliation results, broker CPU and node count, and every config and raw output.
