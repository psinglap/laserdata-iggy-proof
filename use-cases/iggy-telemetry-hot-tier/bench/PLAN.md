# Benchmark plan: telemetry and alerting hot tier

**Question it answers:** at 1 GB/s of logs, metrics and spans, does every event reach the alerter
within 100 ms at p99, stay writable and readable through a rolling restart, and what does each GB
cost to carry, on Apache Iggy and on Kafka tuned for latency, on the same machines?

## Parameters, and why each one

| Parameter | Value | Why |
|---|---|---|
| Event size | 500 B | A structured log line or span with service, status, duration and message. |
| Rate | 200 MB/s → 500 MB/s → 1 GB/s → 2 GB/s | Large platforms ingest GB/s; Shopify reported ~110 GB/s into ClickHouse at its peak. |
| Partitions | 64, keyed by service | One service's events stay together for per-service alert rules. |
| Readers | 3 consumer groups | Alerting on the stream, a ClickHouse sink and an S3 archive. |
| Durability | Iggy replicated × 3 · Kafka acks=all, RF 3, ISR 2 | Telemetry during an incident is the telemetry you most need. |
| Kafka tuning | linger.ms=5, compression none, fetch.max.wait.ms=0 | Small linger is normal for telemetry; same batching on both sides. |
| Incident | 5× for 10 min | When things break, error logs multiply exactly when alerts matter. |
| Rolling restart | Restart each broker in turn at minute 10 | Maintenance must not make recent data unwritable or unqueryable. |
| Cost | Broker CPU, nodes and disk per GB/s | For telemetry, cost per GB usually decides. |
| Duration | 30 min per step + 24 h soak | Telemetry never stops. |
| Runs | 3 per step, median reported | One lucky run is not a result. |

## Hardware

3 × `c6id.4xlarge` brokers (local NVMe), 2 × `c7i.4xlarge` load generators. Same kernel and filesystem.

## Pass lines

1. Ingest-to-alerter p99 under 100 ms at 1 GB/s with 3 readers, replicated, through the incident.
2. Zero seconds with failed produce or stalled consume during the rolling restart.

## What we publish

p99 vs ingest rate (one line per system, 100 ms line drawn), the incident trace, a per-second
produce/consume timeline through the rolling restart, cost per GB/s, and every config and raw output.
