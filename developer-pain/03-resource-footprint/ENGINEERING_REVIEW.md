# Footprint tests: issues for LaserData engineering

Found while measuring memory and CPU of Laser Streams and Laser SDK clients for the footprint blog.
Each item has what happened, how to reproduce it, and where the evidence is. Paths are relative
to this folder. Severity is our guess; please correct it.

## What ran

| | |
|---|---|
| Server | Laser Streams `0.10.1-ld` (`laserdatainc/iggy-server@sha256:348e27e6dca7a1a1b500e89d028dccc61ce7834ea9d76f4e621b3da6ed00a941`) |
| Managed backend | laser-plane `0.41.0` (`laserdatainc/laser-plane@sha256:397d8091ef7467db89205643a0fd61125f34525eabb8b7d674f085e699d8fd19`) |
| Started with | Laser Stack `scripts/up` at commit `46dd016`, plus `stack/limits.yml` (server `cpuset 2,3`, `mem_limit 2g`) |
| Client | Laser SDK `0.9.1` for Python 3.12, SDK defaults, 1 KiB records, batches of 100 |
| Hosts | GitHub `ubuntu-24.04` runners (4 vCPU, kernel 7.0.0-1017-azure), data on an ext4 loop image with barriers; Google Cloud `n2-standard-4` (asia-south1-a), server data on a 100 GB pd-ssd, client on a separate VM |
| Server profiles | `shipped` = Laser Stack as is. `sized-2g` = `IGGY_MEMORY_POOL_SIZE=512 MiB`, `IGGY_PARTITION_WAL_BYTES_MAX=131080 KiB` |

## Summary

| # | Severity | Issue |
|---|---|---|
| 1 | High | Confirmed records lost after the server was OOM-killed during a write burst (shipped budgets, 2 GiB, 50 partitions) |
| 2 | High | One confirmed batch (100 records) lost around a `kill -9`, with sized budgets at 5,000 records/s |
| 3 | Medium | Server memory during write bursts grows with partition count; shipped budgets exceed a 2 GiB node, and the server does not size itself to its cgroup limit |
| 4 | Medium | `partition.wal_bytes_max` cannot go below ~128 MiB per partition, so small nodes cannot bound worst-case WAL memory |
| 5 | Medium | Consumer does not resume after a 60 s server freeze: `the live consumer stream ended` |
| 6 | Medium | Consumer went silent (no error) after a restart and had to be rebuilt |
| 7 | Medium | laser-plane crash-loops on `nobarrier` volumes; Laser Stack `scripts/up` then times out after 120 s |
| 8 | Low | Consumer groups fail with `catalog_unavailable` when laser-plane is not up |
| 9 | Low | Restart takes 8–12 s to reopen ~4 GiB; producer sends block 12–22 s without an error |
| 10 | Low | Background producer reports `unknown outcome` for writes that were in fact stored |
| 11 | Low | Transient `ProtocolError` right after a crash restart |
| 12 | Low | OOM kills are invisible to `docker inspect` (`OOMKilled: false`) because of `init: true` |
| 13 | Low | Startup warnings: missing `core/server/config.toml`, random JWT secret |
| 14 | Low (UX) | Console: signing in with an email whose domain is claimed by an organization shows "No organization yet" with no reason and a dead-end button |

---

## 1. Confirmed records lost after an OOM kill (shipped budgets)

**What happened.** A Laser SDK producer wrote 7,000,000 records (1 KiB) into a 50-partition topic as
fast as it could (about 140,000/s). Server anonymous memory rose from ~130 MiB to 1.3–1.9 GiB, hit
the 2 GiB container limit, and the server was killed and restarted twice (`restart_count: 2`). On
each restart it logged, for every partition:

```
WARN shard-0 journal::partition_journal::segments: discarding unpublished segment bytes beyond the durable WAL frontier group=4294967296 incarnation=7 start_offset=0 durable_bytes=11712704 discarded_bytes=39543808
```

The producer's sends had all been acknowledged (it retried through the restarts and finished). A
fresh consumer then read **2,381,900 of the 7,000,000** acknowledged records. Happened in **2 of 3**
repetitions; the third survived with a 565 MiB peak.

**Reproduce.** `PROFILE=shipped TESTS=backlog-50 ./run.sh` on a 4-core host (server limited to 2 cores, 2 GiB).

**Evidence.** Run [43276020994](https://github.com/psinglap/laserdata-iggy-proof/actions/runs/43276020994),
jobs `rep 1 · backlog-50` and `rep 2 · backlog-50`; per-second memory in
`github/results/shipped/rep-1/samples.csv` (phase `preload-50p`), `server-tail-backlog-50.log`,
`consumer_backlog-50p.json`.

**Questions.** Is an acknowledgement sent before the data is durable in the WAL by default on a
single node? If so, which setting gives "ack after durable"? Should memory growth during a burst be
capped by the pool budget, or is something outside the pool growing?

## 2. One confirmed batch lost around `kill -9` (sized budgets)

**What happened.** Restart test: 4 GiB preloaded, a producer at 5,000 records/s on an 8-partition
topic and a consumer connected, then a graceful restart and a `docker kill -s KILL` + `docker start`.
In **1 of 3** GitHub repetitions, **100 records (sequence 962,700–962,799, one batch)** that the
producer had been told were stored never reached the consumer. By timing, the batch was sent around
the crash restart. Not seen in the other two GitHub runs or the Google Cloud run.

**Reproduce.** `PROFILE=sized-2g TESTS=restart ./run.sh` (repeat; 1 in 3 so far).

**Evidence.** Run [43284394696](https://github.com/psinglap/laserdata-iggy-proof/actions/runs/43284394696),
job `rep 1 · restart`; `github/results/sized-2g/rep-1/restart-consumer.json` (`missing_ranges`),
`restart-producer.json` (stall times), `events.jsonl`, `server-tail-restart.log`.

**Question.** Same mechanism as #1 at small scale? What durability setting would prevent it?

## 3. Server memory under write bursts

**What happened.** Peak server RSS while preloading 7M records:

| Partitions | Shipped budgets | Sized budgets |
|---|---|---|
| 2 | 334 MiB | – |
| 10 | 548 MiB | – |
| 50 | 565 MiB, or past 2 GiB and killed (2 of 3) | 1,029 MiB |

During steady reads the server stays at 150–270 MiB. The default `memory_pool.size` (4 GiB) is twice
the container limit, and nothing in the config reacts to the cgroup limit.

**Questions.** What grows during the burst (pool buffers, WAL, per-partition queues)? Could the
server derive its budgets from the cgroup memory limit, or warn at start-up when budgets exceed it?

## 4. WAL budget floor

`IGGY_PARTITION_WAL_BYTES_MAX=16 MiB` is refused:

```
CONFIG partition.wal_bytes_max must be a 4 KiB multiple between 134225920 and 4294967296 bytes
Error: failed to load server config
```

So the worst case is partitions × ~128 MiB (6.4 GB at 50 partitions), which a small node cannot
hold. **Ask:** a lower floor, or a node-wide WAL cap.

## 5. Consumer stops after a 60 s server freeze

**What happened.** Server frozen with `docker pause` for 60 s while a group consumer was reading.
After unpause the consumer raised:

```
InvalidError: invalid: the live consumer stream ended
```

and received nothing more until the application built a new consumer in the same group. The
rebuilt consumer resumed from the stored offset; nothing lost, 975 records (median) delivered twice.
`consumer.shutdown()` during the outage also raised
`FilterError: filter: unavailable: Request transiently not accepted; retry, on any replica`.

**Evidence.** `github/results/sized-2g/rep-*/producer_pause-consumer.json` (`consumer_errors`,
`consumer_recreated`), `events.jsonl`.

**Question.** Intended? Should the SDK reconnect the live stream itself, as it does after a restart?

## 6. Consumer silent after a restart

In 1 of 3 restart runs the consumer received nothing for over 20 s after a restart, with **no
error raised**, and was rebuilt 3 times by the test's watchdog before records flowed again. In the
other runs it resumed by itself. **Evidence:** `github/results/sized-2g/rep-1/restart-consumer.json`
(`consumer_errors: {}`, `consumer_recreated: 3`).

## 7. laser-plane refuses `nobarrier` volumes

On GitHub `ubuntu-24.04` runners the root filesystem is mounted
`rw,relatime,discard,journal_async_commit,nobarrier,errors=remount-ro,commit=30,data=writeback`.
laser-plane exits in a loop:

```
ERROR plane: the plane volume cannot be used, exiting (reason: the plane volume at /var/lib/laser-plane is mounted with `nobarrier`)
```

and Laser Stack's `scripts/up` fails after 120 s with `plane did not become healthy`. The refusal is
right for production; for Laser Stack (local and CI use) a clearer message from `scripts/up`, or a
documented dev override, would help. Workaround: `stack/data-disk.yml`.

## 8. Consumer groups need laser-plane

Starting a group consumer before laser-plane was up failed with
`FilterError: filter: catalog_unavailable: laser-plane round trip failed: No such file or directory (os error 2)`.
**Question:** should plain streaming consumer groups work without the managed backend (no filter)?

## 9. Restart time and blocked sends

Reopening ~4 GiB: server answering in 8.5–9.6 s (GitHub) and 7.7–12.4 s (Google Cloud, pd-ssd);
producer sends blocked 12–22 s and the consumer resumed after 14–22 s, without errors. With 0.4 GiB
on disk it was ~1 s. **Question:** expected scaling? Any setting that shortens start-up?
**Evidence:** `*/results/sized-2g/README.md`, section 3; `restart-orchestrator.json`.

## 10. `unknown outcome` for stored writes

Background producer during the freeze: 2 writes (1,100 records) failed with
`PublishFailedError('publish failed to fp-pause/events: Operation 17513 has an unknown outcome')` and
`Request transiently not accepted; retry, on any replica`. All 1,100 records were in fact stored and
read. Reporting them is correct; a doc note on how to retry them without duplicates would help.

## 11. Transient `ProtocolError` after a crash

Once, right after `kill -9` + start: `ProtocolError: protocol: filtered page does not match its request
or scan boundary`. The consumer recovered by itself. Evidence: trial run
[43265978452](https://github.com/psinglap/laserdata-iggy-proof/actions/runs/43265978452), `restart-consumer.json`.

## 12. OOM kills invisible to Docker

With Laser Stack's `init: true`, the kernel kills `iggy-server`, not PID 1, so
`docker inspect` shows `OOMKilled: false` and only `RestartCount` changes. Monitoring that relies on
the Docker flag misses it. Worth a note in Laser Stack, or a startup log line on restart after a kill.

## 13. Startup warnings

Every start logs `Configuration file not found at path: 'core/server/config.toml'` and
`JWT encoding and decoding secrets are not configured - generated a random secret`. Harmless for SDK
clients; could be quieter in Laser Stack.

## 14. Console: domain-claimed sign-in is a dead end (UX)

**What happened.** Signing in to laserdata.cloud with a `@laserdata.com` Google account shows:

> **No organization yet** · Your account is not a member of any organization. Ask an organization owner for an invitation. · [Go to your workspace]

The real reason (explained by engineering): the `laserdata.com` domain is claimed by an existing
organization, so accounts on that domain can only be invited or auto-joined, depending on the
owner's settings. The screen doesn't say that, doesn't say who to ask, and "Go to your workspace"
leads nowhere because there is no workspace.

**Suggested copy.**

- Title: **Your email domain belongs to an organization**
- Body: Accounts with a **@laserdata.com** address join the **LaserData** organization; they can't
  create their own. An owner of that organization needs to add you.
- Primary button: **Request to join** (notifies the organization's owners). If auto-join is on:
  **Join LaserData**. After a request: title **Request sent**, button **Check again**, and
  "We've told LaserData's owners. You'll get an email when you're added."
- Secondary: **Sign in with a different account**. Small print: "Why am I seeing this?" (doc link).
- If showing the organization name is a privacy concern, use "an existing organization".
- Remove "Go to your workspace" on this screen.
