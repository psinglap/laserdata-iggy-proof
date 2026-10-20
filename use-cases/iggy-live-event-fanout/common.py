"""Shared settings, message format and latency window for the live-event kit."""
import json
import time

STREAM = "live"
EVENTS = "events"          # scores, odds, suspensions, reaction counts: keyed by match
REACTIONS = "reactions"    # raw viewer reactions: keyed by match
DEFAULT_URL = "iggy+tcp://iggy:iggy@127.0.0.1:8090"


def event(match: str, seq: int, kind: str, **data) -> bytes:
    """One JSON event. seq is per match, so screens can detect gaps and resume after a reconnect."""
    return json.dumps({"m": match, "seq": seq, "t": kind, "ns": time.time_ns(), **data},
                      separators=(",", ":")).encode()


class Window:
    """Rolling one-second latency window, in milliseconds."""

    def __init__(self):
        self.samples, self.start = [], time.monotonic()

    def add(self, ms: float):
        self.samples.append(ms)

    def ready(self) -> bool:
        return time.monotonic() - self.start >= 1.0

    def flush(self) -> dict:
        s = sorted(self.samples) or [0.0]
        pick = lambda q: s[min(len(s) - 1, int(q * len(s)))]
        out = {"n": len(self.samples), "p50": pick(0.5), "p99": pick(0.99), "p999": pick(0.999), "max": s[-1]}
        self.samples, self.start = [], time.monotonic()
        return out
