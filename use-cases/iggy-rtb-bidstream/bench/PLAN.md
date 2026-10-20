# Benchmark plan: real-time bidding event streams

**Question it answers:** can every event in a bidder's money loop (bid log, wins, impressions,
budget pauses) cross the log in under 10 ms at p99 at 1M+ events a second, so pacing and
frequency caps act before the next auction, on Apache Iggy and on Kafka tuned for latency?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Event size | 300 B | A bid, win or impression record with ids, campaign, price and timestamp. |
| Rate | 250K → 1M → 2M → 4M events/s | Large DSPs and SSPs see millions of requests a second; each auction produces several events. |
| Partitions | 32, keyed by user (impressions) or campaign (wins) | Frequency caps need a user's events in order; pacing needs a campaign's. |
| Readers | 8 consumer groups | Pacing, frequency capping, fraud, reporting, billing and bidder caches all read the same events. |
| Durability | Iggy replicated × 3 · Kafka acks=all, RF 3, ISR 2 | Wins are billing records. They cannot be lost. |
| Kafka tuning | linger.ms=0, fetch.max.wait.ms=0 | Kafka on its best latency settings. |
| Burst | 3× for 120 s | Prime time and live sports push ad requests up sharply. |
| Persisted run | 1M/s with fsync before ack | Billing teams sometimes require it. |
| Failover | Kill a leader at minute 15 | Report time until consumers receive again and the next minute's p99. |
| Duration | 30 min per step + 24 h soak | Pacing runs all day; slow drift shows over hours. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Hardware

3 × `c6id.4xlarge` brokers, 2 × `c7i.4xlarge` load generators. Same kernel and filesystem for both.

## Pass line

Produce-to-consume p99 under 10 ms at 2M events/s with 8 readers, replicated, and zero lost wins.

## What we publish

p99 vs event rate (one line per system, the 10 ms per-hop line drawn), p99 through the burst, the
overspend and cap-violation counts from the kit at the same rates, broker CPU and node count, and
every config and raw output.
