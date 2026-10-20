# Add Apache Iggy driver

Apache Iggy graduated to an Apache top-level project in August 2026 and shipped replicated
clustering (Viewstamped Replication) in 0.9.0. People have started comparing it with Kafka, Pulsar
and NATS using Iggy's own bench tool, which makes those comparisons hard to trust. This adds a driver
so Iggy can be measured with the same framework and workloads as every other system here.

## What's included

- `driver-iggy`: driver, producer, consumer and config, using the official `org.apache.iggy:iggy`
  0.9.0 Java SDK (async TCP client).
- `driver-iggy/iggy.yaml`: driver config with Iggy's defaults.
- Registration in the root `pom.xml` modules and in `benchmark-framework/pom.xml`.

## Design notes

- Subscriptions map to Iggy consumer groups, so `consumerPerSubscription` behaves as it does for Kafka.
- The publish timestamp travels in Iggy's per-message origin timestamp, so end-to-end latency is
  measured from the producer, as the Kafka driver does with record create time.
- Messages are sent one at a time; no client-side batching is added.

## Testing

- [x] Compiles against `org.apache.iggy:iggy:0.9.0`.
- [ ] `simple-workload` against a single Iggy 0.9.0 node. <!-- run before opening the PR -->
- [ ] 30-minute `latency-under-5ms` against a 3-node cluster. <!-- run before opening the PR -->

Disclosure: I work at LaserData, which builds and runs Apache Iggy. Happy to maintain this driver.
