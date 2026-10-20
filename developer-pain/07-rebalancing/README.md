> **LaserData proof · Developer pain 07: Consumer-group rebalancing & stalls**  
> Blog: [How Apache Iggy moves partitions between consumers without stopping the group](https://claude.ai/artifact/7pcPmhKyZC9Y1EBWqxa2ar) · Doc: [How consumer groups work in Apache Iggy](https://claude.ai/artifact/Stm1Vo6iaRreBA8LrrBjCh) · Posts: [ready-to-publish drafts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#rebalance)  
> Where this code goes: LaserData repo: a consumer-group churn test kit.  
> Status: not yet run against a live Apache Iggy server unless a checklist below says so. Nothing has been opened upstream.

# iggy-rebalance-storm

A test kit from **LaserData** that shows what happens to an Apache Iggy consumer group while its
members keep changing. A producer writes numbered messages at a steady rate. A controller changes
the group every 10 seconds: a member leaves cleanly, a member is killed, or a new member joins.
Every delivery is logged, so you can measure exactly what the change cost.

## What it measures

| Measure | How | Why it matters |
|---|---|---|
| Longest partition pause | Longest time any partition delivered nothing, baseline vs churn | How long a rebalance stops work |
| Duplicates | Messages delivered more than once | Work your handlers must make idempotent |
| Missing | Messages sent but never delivered | Should be zero under at-least-once |
| End-to-end p50 / p99 / max | Send time in the payload vs receive time | What your users feel during churn |

## Run it

```bash
docker compose up -d                 # Apache Iggy 0.9.0 with the io_uring settings it needs
pip install -r requirements.txt
python storm.py --mode mixed --consumers 4 --partitions 12 --rate 5000 --seconds 300
python analyze.py run/
```

Modes: `leave` (SIGTERM, the consumer calls `leave_consumer_group`), `kill` (SIGKILL, so no
leave call, but the OS closes the socket), `freeze` (SIGSTOP, like a hung host: the socket stays
open and nothing pings), `join` (scale out), `mixed` (a seeded random mix of all four).
Add `--work-ms 5` to simulate handling time, which makes in-flight work visible.

## What to expect, and why

Apache Iggy assigns partitions on the server. In 0.9:

- **A member joins.** Partitions with no owner go to the new member at once. A busy member's
  surplus partitions are marked for handoff and move only once that member has committed what it
  already polled, or after `consumer_group.rebalancing_timeout` (30 s by default). Expect few or
  no duplicates.
- **A member leaves cleanly.** The server reassigns the group's partitions straight away.
  Messages the leaving member polled but did not commit are delivered again to the new owner.
- **A member is killed.** Its socket closes, the server drops it from the group and reassigns,
  as for a clean leave.
- **A member hangs.** The socket stays open, so the server waits for the session to expire. The
  SDKs ping every 5 s, and a session ends after 1.2 x `heartbeat.interval` (36 s by default) with
  no traffic. Expect the longest pause here on that member's partitions; lower
  `heartbeat.interval` to shorten it.

## Results

Not run yet. When you run it, commit `run/results.json` together with the machine type, the
Iggy version and the command line, so others can reproduce it.

## License

Apache License 2.0. Maintained by LaserData, which builds and runs Apache Iggy.
