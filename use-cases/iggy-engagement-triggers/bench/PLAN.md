# Benchmark plan: engagement events and real-time triggers

**Question it answers:** when a campaign send makes millions of users open the app at once, do
engagement events still reach the trigger engine and the other readers within 50 ms at p99, so
in-session messages fire while users are still there, and what does each billion events cost,
on Apache Iggy and on Kafka tuned for latency?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Event size | 300 B | User, sequence, event type, properties and timestamp. |
| Rate | 100K → 500K → 1M → 2M events/s | Engagement platforms ingest billions of events a day; Klaviyo published 170K/s after its rebuild. |
| Partitions | 64, keyed by user | One user's actions must arrive in order for triggers to be correct. |
| Readers | 5 consumer groups | Triggers, segments, personalization, analytics and a warehouse sink. |
| Durability | Iggy replicated × 3 · Kafka acks=all, RF 3, ISR 2 | Purchases and consent changes cannot be lost. |
| Kafka tuning | linger.ms=0, fetch.max.wait.ms=0 | Kafka on its best latency settings. |
| Campaign blast | 10× for 10 min | A send to millions makes them open the app at the same moment. |
| Cost | Broker CPU and nodes per step | At billions of events a day, cost per billion decides the architecture. |
| Failover | Kill a leader at minute 15 | Report time until readers receive again and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Engagement never stops; daily patterns show over 24 h. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Hardware

3 × `c6id.4xlarge` brokers, 2 × `c7i.4xlarge` load generators. Same kernel and filesystem for both.

## Pass lines

1. Event-to-reader p99 under 50 ms at 1M events/s with 5 readers, replicated, through the blast.
2. In the kit at the same rate, in-session trigger p99 under 1 s and cart reminders late by under 1 s at p99.

## What we publish

p99 vs event rate (one line per system, 50 ms line drawn), the blast trace, broker CPU and node
count per million events per second, the kit's trigger numbers, and every config and raw output.
