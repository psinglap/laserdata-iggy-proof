# Benchmark plan: AI agent event bus

**Question it answers:** with 25,000 concurrent voice calls, does each hop on the agent bus stay under
10 ms at p99, so a turn that crosses the bus four times spends under 40 ms of its ~800 ms budget on
orchestration, on Apache Iggy and on Kafka tuned for latency, on the same machines?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Event size | 1 KB | Transcript chunks, tool calls and results are small JSON documents. |
| Rate | 25K → 100K → 250K → 500K events/s | 2,500 to 50,000 concurrent calls at about 10 bus events per call per second. |
| Partitions | 64, keyed by call | A call's events must stay in order, and with one agent. |
| Readers | 4 consumer groups | Agents, tool workers, the telephony gateway and evals/audit. |
| Durability | Iggy replicated × 3 · Kafka acks=all, RF 3, ISR 2 | A call must survive an agent or node crash mid-sentence. |
| Kafka tuning | linger.ms=0, fetch.max.wait.ms=0 | Kafka on its best latency settings. |
| Call surge | 3× for 10 min | Outages, launches and campaigns flood contact centers at once. |
| Failover | Kill a leader at minute 15 | Report time until agents receive again and how many turns exceeded budget. |
| Duration | 30 min per step + 24 h soak | Contact centers run around the clock. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Hardware

3 × `c6id.4xlarge` brokers, 2 × `c7i.4xlarge` load generators. Same kernel and filesystem for both.

## Pass lines

1. Per-hop p99 under 10 ms at 250K events/s with 4 readers, replicated, through the surge.
2. In the kit at the same rate, orchestration overhead per turn under 40 ms at p99.

## What we publish

Per-hop p99 vs event rate (one line per system, 10 ms line drawn), the surge trace, the kit's turn and
orchestration-overhead numbers, broker CPU and node count, and every config and raw output.
