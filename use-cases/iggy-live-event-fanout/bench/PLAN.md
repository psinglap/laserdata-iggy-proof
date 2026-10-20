# Benchmark plan: live scores, odds and reactions

**Question it answers:** when a goal is scored and every match updates at once, does every push
gateway still receive every event within 100 ms p99, and can reactions be ingested at 2M a second
without falling behind, on Apache Iggy and on Kafka tuned for latency, on the same machines?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Event size | 200 B JSON | A score, odds or suspension event with match id, sequence and timestamp. |
| Event rate | 10K events/s across 500 matches | A busy evening of live sport: odds move constantly, scores rarely. |
| Fan-out | 10 → 50 → 100 → 200 gateways, one consumer group each | Every gateway needs every event. This is the number that grows with audience. |
| The big moment | 20× events for 30 s | A goal or wicket: suspensions, a score and a burst of odds moves, everywhere at once. |
| Reactions | 100K → 500K → 1M → 2M taps/s, 80 B | Tens of millions of viewers, a small share tapping in the seconds after a goal. |
| Partitions | 8 for events, 16 for reactions, keyed by match | Per-match order must hold; reactions spread for ingest. |
| Durability | Iggy replicated × 3 · Kafka acks=all, RF 3, ISR 2 | The odds suspension must not be lost. |
| Kafka tuning | linger.ms=0, fetch.max.wait.ms=0 | Kafka on its best latency settings. |
| Duration | 30 min per step + one 4 h soak | A full match, with the big moments inside it. |
| Failover | Kill the events leader at minute 15 | Report time until gateways receive again and the next minute's p99. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Hardware

3 × `c6id.4xlarge` brokers, 2 × `c7i.4xlarge` load generators (producers and gateway consumers on
the same host so clocks agree). Same kernel and filesystem for both systems.

## Pass lines

1. Event-to-gateway p99 under 100 ms with 200 gateways, through the 20× burst, with zero gaps.
2. Reaction ingest lag p99 under 250 ms (one aggregation window) at 2M taps/s.

## What we publish

p99 vs number of gateways (one line per system), the per-second p99 trace through the burst,
reaction lag vs rate, broker CPU and node count, and every config and raw output.
