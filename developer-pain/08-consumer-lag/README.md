> **LaserData proof · Developer pain 08: Lag & observability**  
> Blog: [Is your consumer keeping up? Consumer lag for Apache Iggy, in Prometheus and Grafana](https://claude.ai/artifact/W8pKGcPYFD3kVxe9jBR9Kz) · Doc: [Monitor consumer lag in Apache Iggy](https://claude.ai/artifact/MrTWgMAbMGTCKoh3hi9GPh) · Posts: [ready-to-publish drafts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#lag)  
> Where this code goes: LaserData repo: exporter and Grafana dashboard. The metric proposal goes to apache/iggy as an issue or pull request.  
> Status: not yet run against a live Apache Iggy server unless a checklist below says so. Nothing has been opened upstream.

# iggy-lag-exporter

A Prometheus exporter for **Apache Iggy** consumer-group lag, from **LaserData**. It answers the
question every on-call engineer asks first: *is this consumer keeping up?*

Apache Iggy stores each consumer group's committed offset on the server, but its `/metrics`
endpoint doesn't export consumer progress yet. This exporter reads it and exports:

| Metric | Labels | Meaning |
|---|---|---|
| `iggy_consumer_group_lag` | stream, topic, group, partition | Messages after the group's last commit |
| `iggy_consumer_group_committed_offset` | stream, topic, group, partition | The group's committed offset |
| `iggy_partition_current_offset` | stream, topic, partition | Offset of the newest message |
| `iggy_consumer_group_members` | stream, topic, group | Members in the group |
| `iggy_lag_exporter_up`, `_scrape_errors_total`, `_collect_seconds` | | Exporter health |

It ships with a Grafana dashboard (`grafana/iggy-consumer-lag.json`) and alert rules
(`prometheus/alerts.yml`) for "lag is growing" and "the group stopped committing".

## Run it

```bash
pip install -r requirements.txt
python iggy_lag_exporter.py --url iggy+tcp://lag-reader:secret@iggy:8090 --port 9108
curl localhost:9108/metrics
```

Or with Docker: `docker build -t iggy-lag-exporter . && docker run -p 9108:9108 iggy-lag-exporter --url ...`

Use a dedicated user with read permissions on the streams you want to watch.

## How it works

Every `--interval` seconds (15 by default) it lists streams, topics and consumer groups with the
Apache Iggy Python SDK, reads each topic's partitions (current offset and message count), and asks
the server for each group's committed offset per partition. The 0.9 Python SDK has no call for a
group's offset (apache/iggy#3997), so the exporter sends the `GetConsumerOffset` command (code 120)
through `send_binary_request`, encoded exactly as in `core/binary_protocol`. Unit tests in `tests/`
check the encoding.

Lag is `current_offset - committed_offset`, 0 for an empty partition, and the partition's message
count for a group that has never committed.

**Cost:** one request per group per partition per interval. For very large topics, raise
`--interval`. We have proposed exporting these series from the server itself
(`upstream/ISSUE_consumer_group_lag_metric.md`), which would make this exporter unnecessary.

## Status

- [x] Unit tests for the wire encoding and the lag rule (`pytest tests`)
- [x] Dashboard JSON and alert rules parse
- [ ] Run against a live Apache Iggy 0.9 server
- [ ] Dashboard imported into Grafana and screenshotted

## License

Apache License 2.0. Maintained by LaserData, which builds and runs Apache Iggy.
