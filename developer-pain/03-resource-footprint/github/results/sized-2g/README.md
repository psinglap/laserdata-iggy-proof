# Footprint results

Measured 2026-11-12 by [this GitHub Actions run](https://github.com/psinglap/laserdata-iggy-proof/actions/runs/43284394696), 3 repetitions, each on a fresh GitHub-hosted machine (4 vCPU AMD EPYC 9V74 80-Core Processor, 15 GiB, kernel 7.0.0-1017-azure).

Server: Laser Streams `server 0.10.1-ld` (`laserdatainc/iggy-server@sha256:348e27e6dca7a1a1b500e89d028dccc61ce7834ea9d76f4e621b3da6ed00a941`), from Laser Stack commit `46dd016`, LaserData's shipped settings, limited to 2g of memory and 2 dedicated cores. Clients: Laser SDK 0.9.1 for Python, SDK default settings, on separate cores.

Each cell is the median across repetitions, with the lowest and highest in brackets. Memory is resident memory (RSS) in MiB unless it says container, which also counts the page cache the kernel keeps for the server's files. CPU is percent of one core.

## Idle

| Server RSS | Server container | Server CPU | Managed backend RSS |
|---|---|---|---|
| 92 (92–92) | 71 (71–71) | 0.9% (1–1) | 56 (56–59) |

## 1. Consumer reading a 7 million record backlog

| Partitions | Consumer RSS peak | Consumer RSS median | Server RSS peak | Server container peak | Drain time (s) | Records/s | Missing |
|---|---|---|---|---|---|---|---|
| 2 | 73 (73–73) | 72 (72–72) | 150 (150–151) | 2,048 | 949 (620–1,103) | 7,379 (6,346–11,289) | 0 |
| 10 | 72 (72–72) | 72 (72–72) | 181 (178–183) | 2,048 | 740 (405–932) | 9,458 (7,511–17,275) | 0 |
| 50 | 72 (72–72) | 72 (68–72) | 264 (208–283) | 2,048 | 639 (511–1,111) | 10,959 (6,299–13,705) | 0 |

## 2. Producer for 30 minutes at 8,000 records/s, server paused for 60 s halfway

| Producer RSS before | Peak while paused | Peak after resume | Last 2 minutes | Growth before pause (MiB/h) | Growth after (MiB/h) | Failed writes | Missing |
|---|---|---|---|---|---|---|---|
| 59 (58–61) | 91 (91–92) | 91 (91–92) | 84 (70–86) | 4.2 (2–10) | 0 | 2 | 0 |

## 3. Server restart with 4 GiB on disk, producer and consumer connected

Steady before restart: server RSS 119 (117–125), CPU 9.9% (10–14).

| Restart | Server answers (s) | Producer writes again (s) | Consumer reads again (s) | Server RSS peak, first 60 s | Container peak | CPU peak |
|---|---|---|---|---|---|---|
| graceful | 9.6 (9–10) | 12 (11–30) | 15 (14–16) | 164 (164–169) | 2,048 (2,048–2,048) | 26% (25–27) |
| crash | 8.5 (8–10) | 14 (14–20) | 14 (14–16) | 160 (159–163) | 2,048 (2,048–2,048) | 25% (22–28) |

Confirmed records missing after both restarts: 0 (0–100).

Raw evidence for every repetition is in `rep-*/`: `samples.csv` (one row per second per process), `events.jsonl`, one JSON per test, `env.json` and the tail of the server log.
