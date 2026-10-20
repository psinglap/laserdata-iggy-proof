# Apache Iggy driver

Benchmarks [Apache Iggy](https://iggy.apache.org) with the OpenMessaging Benchmark framework.

## Mapping

| OpenMessaging Benchmark | Apache Iggy |
|---|---|
| topic | topic inside one stream (`streamName`, default `omb`) |
| partitions | topic partitions |
| subscription | consumer group (consumers in a subscription share partitions) |
| message key | `Partitioning.messagesKey(key)`; no key = balanced |
| publish timestamp | message origin timestamp, set by the producer |

Each producer and consumer opens its own TCP connection with Iggy's async Java SDK.
Producers send each message as it arrives, so the driver measures per-message latency,
not batching. Consumers poll the next messages for their group with auto-commit.

## Run

```bash
# Iggy 0.9.0 on the broker hosts
docker run -d --network host --cap-add SYS_NICE --security-opt seccomp=unconfined \
  --ulimit memlock=-1:-1 -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy apache/iggy:0.9.0

bin/benchmark --drivers driver-iggy/iggy.yaml workloads/latency-under-5ms.yaml
```

Durability is set per topic on the Iggy server (`replicated` by default, or `persisted`).
State which one you ran when you publish results.
