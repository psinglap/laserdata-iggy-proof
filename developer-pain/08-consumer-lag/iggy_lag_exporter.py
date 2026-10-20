"""Prometheus exporter for Apache Iggy consumer-group lag.

Every scrape interval it walks every stream, topic and consumer group, reads each
group's committed offset per partition, and exports how far it is behind the end
of the partition. From LaserData, which builds and runs Apache Iggy. Apache License 2.0.
"""
import argparse
import asyncio
import logging
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from apache_iggy import IggyClient

log = logging.getLogger("iggy-lag-exporter")

# Binary protocol, as in apache/iggy core/binary_protocol (unchanged since 0.9.0).
GET_CONSUMER_OFFSET_CODE = 120
CONSUMER_KIND_GROUP = 2
IDENTIFIER_NUMERIC = 1


def numeric_identifier(value: int) -> bytes:
    return struct.pack("<BBI", IDENTIFIER_NUMERIC, 4, value)


def encode_get_group_offset(stream_id: int, topic_id: int, group_id: int, partition_id: int) -> bytes:
    """GetConsumerOffset: consumer (kind + id), stream id, topic id, partition flag + id."""
    return (
        struct.pack("<B", CONSUMER_KIND_GROUP)
        + numeric_identifier(group_id)
        + numeric_identifier(stream_id)
        + numeric_identifier(topic_id)
        + struct.pack("<BI", 1, partition_id)
    )


def decode_offset(payload: bytes):
    """Returns (partition_id, current_offset, stored_offset), or None if nothing is stored."""
    if not payload:
        return None
    return struct.unpack_from("<IQQ", payload, 0)


def lag(current_offset: int, messages_count: int, stored_offset) -> int:
    """Messages after the group's last commit. Offsets are message offsets, so a group
    that committed the last message has lag 0. A group that never committed has the
    whole partition ahead of it."""
    if messages_count == 0:
        return 0
    if stored_offset is None:
        return messages_count
    return max(0, current_offset - stored_offset)


def label(value) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class Metrics:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.body = b""
        self.errors = 0

    def publish(self, lines) -> None:
        with self.lock:
            self.body = ("\n".join(lines) + "\n").encode()

    def render(self) -> bytes:
        with self.lock:
            return self.body


async def collect(client: IggyClient) -> list:
    lag_rows, offset_rows, end_rows, member_rows = [], [], [], []
    for stream in await client.get_streams():
        for topic in await client.get_topics(stream.id):
            groups = await client.get_consumer_groups(stream.id, topic.id)
            if not groups:
                continue
            details = await client.get_topic(stream.id, topic.id)
            if details is None:
                continue
            for partition in details.partitions:
                end_rows.append((stream.name, topic.name, partition.id, partition.current_offset))
            for group in groups:
                member_rows.append((stream.name, topic.name, group.name, group.members_count))
                for partition in details.partitions:
                    payload = await client.send_binary_request(
                        GET_CONSUMER_OFFSET_CODE,
                        encode_get_group_offset(stream.id, topic.id, group.id, partition.id),
                    )
                    info = decode_offset(payload)
                    stored = info[2] if info else None
                    labels = (stream.name, topic.name, group.name, partition.id)
                    lag_rows.append(labels + (lag(partition.current_offset, partition.messages_count, stored),))
                    if stored is not None:
                        offset_rows.append(labels + (stored,))

    def series(name, rows, keys):
        for row in rows:
            pairs = ",".join(f'{k}="{label(v)}"' for k, v in zip(keys, row[:-1]))
            yield f"{name}{{{pairs}}} {row[-1]}"

    g = ("stream", "topic", "group", "partition")
    return [
        "# HELP iggy_consumer_group_lag Messages after the group's last committed offset.",
        "# TYPE iggy_consumer_group_lag gauge",
        *series("iggy_consumer_group_lag", lag_rows, g),
        "# HELP iggy_consumer_group_committed_offset The group's committed offset.",
        "# TYPE iggy_consumer_group_committed_offset gauge",
        *series("iggy_consumer_group_committed_offset", offset_rows, g),
        "# HELP iggy_partition_current_offset Offset of the last message in the partition.",
        "# TYPE iggy_partition_current_offset gauge",
        *series("iggy_partition_current_offset", end_rows, ("stream", "topic", "partition")),
        "# HELP iggy_consumer_group_members Members currently in the group.",
        "# TYPE iggy_consumer_group_members gauge",
        *series("iggy_consumer_group_members", member_rows, ("stream", "topic", "group")),
    ]


async def run(url: str, interval: float, metrics: Metrics) -> None:
    client = IggyClient.from_connection_string(url)
    await client.connect()
    while True:
        started = time.monotonic()
        try:
            lines = await collect(client)
            ok = 1
        except Exception:  # keep serving the last good data and count the failure
            log.exception("scrape failed")
            metrics.errors += 1
            lines, ok = None, 0
        took = time.monotonic() - started
        tail = [
            "# HELP iggy_lag_exporter_up 1 if the last collection succeeded.",
            "# TYPE iggy_lag_exporter_up gauge",
            f"iggy_lag_exporter_up {ok}",
            "# HELP iggy_lag_exporter_scrape_errors_total Failed collections since start.",
            "# TYPE iggy_lag_exporter_scrape_errors_total counter",
            f"iggy_lag_exporter_scrape_errors_total {metrics.errors}",
            "# HELP iggy_lag_exporter_collect_seconds Time the last collection took.",
            "# TYPE iggy_lag_exporter_collect_seconds gauge",
            f"iggy_lag_exporter_collect_seconds {took:.3f}",
        ]
        if lines is not None:
            metrics.publish(lines + tail)
        await asyncio.sleep(max(0.0, interval - took))


def serve(port: int, metrics: Metrics) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            body = metrics.render()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--url", default="iggy+tcp://iggy:iggy@127.0.0.1:8090",
                    help="TCP connection string; use a read-only user")
    ap.add_argument("--port", type=int, default=9108)
    ap.add_argument("--interval", type=float, default=15.0, help="seconds between collections")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    metrics = Metrics()
    threading.Thread(target=serve, args=(args.port, metrics), daemon=True).start()
    log.info("serving /metrics on :%d", args.port)
    asyncio.run(run(args.url, args.interval, metrics))


if __name__ == "__main__":
    main()
