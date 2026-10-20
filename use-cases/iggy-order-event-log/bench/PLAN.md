# Benchmark plan: order and trade event log

**Question it answers:** with the log as the write-ahead record of every order, how long does a
broker's customer wait for an order acknowledgement at the market open, how fast can positions be
rebuilt from the log, on Apache Iggy and on Kafka tuned for latency, on the same machines?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Event size | 300 B | Account, sequence, order id, lifecycle step, symbol, quantity and price. |
| Rate | 10K → 50K → 100K → 250K orders/s | Retail brokers and exchanges see tens of thousands of orders a second at the open. |
| Batching | 1 order per write | Each order is acknowledged individually; no batching delay is allowed. |
| Partitions | 32, keyed by account | An account's orders, fills and cancels stay in one ordered history. |
| Readers | 4 consumer groups | Venue gateway, positions, risk and surveillance. |
| Durability | Iggy replicated × 3 · Kafka acks=all, RF 3, ISR 2, idempotent | The order log must survive a node failure. |
| Persisted run | 100K/s with fsync before ack | Some brokers require the order on disk before acknowledging. |
| Market open | 5× for 60 s | The first minute after the bell. |
| Rebuild | Replay a full trading day (≈ 1B events) into positions | Recovery time after a crash, and how fast new services can start. |
| Failover | Kill a leader during the open | Report the longest order acknowledgement and any lost or duplicated orders. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Hardware

3 × `c6id.4xlarge` brokers, 2 × `c7i.4xlarge` load generators. Same kernel and filesystem for both.

## Pass lines

1. Order acknowledgement p99 under 5 ms at 100K orders/s, replicated, through the open.
2. Zero lost or duplicated orders across the failover; a day's log rebuilds positions in minutes, not hours.

## What we publish

Acknowledgement p99 vs order rate (one line per system, 5 ms line drawn), the open trace, the
persisted run, rebuild time, failover results, broker CPU and node count, and every config and raw output.
