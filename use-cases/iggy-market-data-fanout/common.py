"""Shared tick format and latency stats for the market-data fan-out kit."""
import struct
import time

# symbol(8) seq(8) sent_ns(8) bid(8) ask(8) bid_qty(4) ask_qty(4) = 48 bytes, padded to --size
TICK = struct.Struct("<8sQQddII")
STREAM, TOPIC = "market", "ticks"
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090"


def pack(symbol: str, seq: int, bid: float, ask: float, size: int) -> bytes:
    body = TICK.pack(symbol.encode()[:8].ljust(8), seq, time.time_ns(), bid, ask, 100, 100)
    return body.ljust(size, b"\0")


def unpack(payload: bytes):
    sym, seq, sent_ns, bid, ask, _, _ = TICK.unpack_from(payload)
    return sym.rstrip(b"\0 ").decode(), seq, sent_ns, bid, ask


class Window:
    """Rolling one-second latency window: p50 / p99 / p99.9 / max in microseconds."""

    def __init__(self):
        self.samples, self.start = [], time.monotonic()

    def add(self, us: float):
        self.samples.append(us)

    def ready(self) -> bool:
        return time.monotonic() - self.start >= 1.0

    def flush(self) -> dict:
        s = sorted(self.samples) or [0.0]
        pick = lambda q: s[min(len(s) - 1, int(q * len(s)))]
        out = {"n": len(self.samples), "p50": pick(0.50), "p99": pick(0.99),
               "p999": pick(0.999), "max": s[-1]}
        self.samples, self.start = [], time.monotonic()
        return out
