# feat(server): export consumer-group lag on the Prometheus endpoint

### Description

The server's Prometheus endpoint (`[http.metrics]`, `/metrics`) exports counts (`streams`,
`topics`, `partitions`, `segments`, `messages`, `users`, `clients`) and per-topic series
(`topic_size_bytes`, `topic_messages`, `topic_max_size_bytes`, on main since 0.9.0). It has nothing about consumer
progress, so a group that falls behind or stops committing is invisible to Prometheus and Grafana.

The data is already on the server. Each partition knows its current offset, and the partition
plane stores each consumer group's committed offset. `GetConsumerOffset` returns both together
(`ConsumerOffsetInfo { partition_id, current_offset, stored_offset }`). Today, an operator who wants
lag must run a separate client that walks every stream, topic, group and partition over TCP and
issues one `GetConsumerOffset` per partition per group. We built such an exporter
([iggy-lag-exporter](LINK)) and it works, but it adds a process, credentials and N x M requests
per scrape that the server could answer from memory.

Related: #4447, #4462 and #4489 (lag in the CLI), #4279 (group offsets over HTTP), #3997 (offset
APIs in the Python SDK). Those make lag visible to a person; this makes it visible to alerting.

### Affected area / component

Server (`core/server/src/http/metrics.rs`); partitions (committed offsets per group).

### Proposed solution

Add three gauge families, exported only to users with `read_servers` or `manage_servers`, like the
existing per-topic series:

```
partition_current_offset{stream, topic, partition}
consumer_group_committed_offset{stream, topic, group, partition}
consumer_group_lag{stream, topic, group, partition}
```

`consumer_group_lag` is the number of messages after the group's committed offset: 0 for an empty
partition, the partition's message count when the group has never committed, and otherwise
`current_offset - committed_offset`.

Cardinality is groups x partitions per topic. To keep large deployments safe, put the group series
behind an opt-in setting, for example:

```toml
[http.metrics]
consumer_group_lag = false   # export per-group, per-partition lag gauges
```

Optional follow-up: a time-based lag (`consumer_group_lag_seconds`, the age of the oldest
uncommitted message), which is what most alerts actually want. It needs the timestamp of the
message after the committed offset, so it can come later.

### Alternatives considered

- **An external exporter** (what we run today). Works on 0.9, but adds a process, a TCP client and
  one request per group per partition per scrape.
- **Lag in the CLI only.** Good for people, not for alerts.
- **Computing lag in Grafana** from `topic_messages` and a group offset. Not possible: no group
  offset is exported.

### Willing to contribute

Yes. LaserData can open the pull request, including tests in the style of the existing
`metrics.rs` tests and a docs update.

I work at LaserData, which builds and runs Apache Iggy.
