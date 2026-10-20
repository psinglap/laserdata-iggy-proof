"""Shared settings for the AI agent bus kit."""
import json
import time

STREAM = "agents"
SESSION = "session"   # every event of every call, keyed by call id
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090"


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


class Window:
    def __init__(self):
        self.samples, self.start = [], time.monotonic()

    def add(self, v):
        self.samples.append(v)

    def ready(self):
        return time.monotonic() - self.start >= 1.0

    def flush(self):
        s = sorted(self.samples) or [0.0]
        pick = lambda q: s[min(len(s) - 1, int(q * len(s)))]
        out = {"n": len(self.samples), "p50": pick(0.5), "p99": pick(0.99), "p999": pick(0.999), "max": s[-1]}
        self.samples, self.start = [], time.monotonic()
        return out


async def bus(client, partitions=16):
    """Producer for the session topic. Every event of a call goes to one partition, in order."""
    from datetime import timedelta
    from apache_iggy import DirectProducerConfig, Durability
    return await client.producer(STREAM, SESSION, topic_partitions_count=partitions, topic_durability=Durability.REPLICATED,
                                 mode=DirectProducerConfig(batch_length=64, linger_time=timedelta(microseconds=100)))
