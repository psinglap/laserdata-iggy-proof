"""Shared settings for the location, dispatch and ETA kit."""
import json
import math
import time

STREAM = "mobility"
PINGS, ORDERS, ETA = "pings", "orders", "eta"
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090"
QUIC_URL = "iggy+quic://iggy:iggy@127.0.0.1:8080"
CITY = (40.70, -74.02, 40.82, -73.92)   # a box over Manhattan: lat_min, lon_min, lat_max, lon_max
CELL = 0.005                              # ~500 m grid cells for nearest-courier search


def msg(**d) -> bytes:
    d.setdefault("ns", time.time_ns())
    return json.dumps(d, separators=(",", ":")).encode()


def load(b: bytes) -> dict:
    return json.loads(b)


def cell(lat: float, lon: float):
    return (int(lat / CELL), int(lon / CELL))


def km(a_lat, a_lon, b_lat, b_lon) -> float:
    dy, dx = (a_lat - b_lat) * 111.0, (a_lon - b_lon) * 111.0 * math.cos(math.radians(a_lat))
    return math.hypot(dx, dy)


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
