# Benchmark plan: edge store-and-forward

**Question it answers:** on a small ARM gateway, how much memory and CPU does the local log use at
10,000 readings a second, how fast does it forward a one-hour backlog after an outage, and is every
reading delivered in order with nothing lost, for Apache Iggy, a single-node Kafka (KRaft) and NATS
JetStream as a leaf node, on the same hardware?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Gateway | 4-core ARM64, 4 GB RAM (e.g. AWS `t4g.medium` or a Raspberry Pi 5), local SSD | A typical store, vehicle or plant gateway. |
| Reading size | 150 B | Device, sequence, a few sensor values and a timestamp. |
| Rate | 1K → 5K → 10K → 20K readings/s per gateway | From a small store to a busy plant floor. |
| Durability on the gateway | Persisted (fsync before ack) | Power cuts are common at the edge; readings must survive them. |
| Outage | Link down for 10 min, 1 h and 6 h | Vehicles in tunnels, stores on bad links, ships at sea. |
| Measures | Idle and loaded RSS, CPU, catch-up rate, edge-to-cloud p99 when live, gaps and repeats | Footprint and correctness are what decide edge deployments. |
| Comparison | Iggy · Kafka single-node KRaft · NATS JetStream leaf node | Kafka is the familiar default; NATS leaf nodes are the established edge pattern. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Pass lines

1. Iggy on the gateway stays within a 512 MB memory limit at 10K readings/s.
2. After a 1 h outage at 10K readings/s, the backlog (36M readings) forwards with 0 gaps, in order per device.

## What we publish

Memory and CPU vs rate for each system, catch-up time for each outage length, edge-to-cloud p99 when
live, gaps and repeats, and every config and raw output. If NATS or Kafka is smaller or faster on a
measure, that row is published.
