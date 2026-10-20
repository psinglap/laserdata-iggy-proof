"""Shared settings for the usage metering and quota kit."""
import json
import time

STREAM = "metering"
USAGE, ROLLUPS, QUOTA = "usage", "rollups", "quota"
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
