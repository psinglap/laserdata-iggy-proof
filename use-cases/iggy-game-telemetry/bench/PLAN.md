# Benchmark plan: game telemetry and live-ops

**Question it answers:** at a million gameplay events a second, does every event reach the
leaderboard, economy and anti-cheat readers within 50 ms at p99, while game servers hand events
off without ever stalling a 7.8 ms tick, on Apache Iggy and on Kafka tuned for latency?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Event size | 200 B | Match, player, event type, tick and a few values. |
| Rate | 100K → 500K → 1M → 2M events/s | Large titles run tens of thousands of matches at once, each emitting events every tick. |
| Partitions | 64, keyed by match | A match's events stay in order for replays and anti-cheat. |
| Readers | 4 consumer groups | Leaderboards, economy watch, anti-cheat and the analytics warehouse. |
| Durability | Iggy replicated × 3 · Kafka acks=all, RF 3, ISR 2 | Economy and anti-cheat evidence cannot be lost. |
| Producer mode | Background (non-blocking), 5 ms linger, both sides | The game loop must never wait on the network. |
| Tournament start | 5× matches for 10 min | Season launches and tournaments start everyone at once. |
| Failover | Kill a leader at minute 15 | Report time until readers receive again, and producer drops on the game side. |
| Duration | 30 min per step + 24 h soak | Live-service games never close. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Hardware

3 × `c6id.4xlarge` brokers, 2 × `c7i.4xlarge` load generators. Same kernel and filesystem for both.

## Pass lines

1. Event-to-reader p99 under 50 ms at 1M events/s with 4 readers, replicated, through the tournament start.
2. In the kit, zero tick overruns caused by event handoff, and zero dropped events at the same rate.

## What we publish

p99 vs event rate (one line per system, 50 ms line drawn), the tournament trace, producer drops during
failover, the kit's tick and handoff numbers, broker CPU and node count, and every config and raw output.
