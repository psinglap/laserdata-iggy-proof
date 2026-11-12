# Footprint results

Measured 2026-11-12 by [this GitHub Actions run](https://github.com/psinglap/laserdata-iggy-proof/actions/runs/43276020994), 3 repetitions, each on a fresh GitHub-hosted machine (4 vCPU AMD EPYC 9V74 80-Core Processor, 15 GiB, kernel 7.0.0-1017-azure).

Server: Laser Streams `server 0.10.1-ld` (`laserdatainc/iggy-server@sha256:348e27e6dca7a1a1b500e89d028dccc61ce7834ea9d76f4e621b3da6ed00a941`), from Laser Stack commit `46dd016`, LaserData's shipped settings, limited to 2g of memory and 2 dedicated cores. Clients: Laser SDK 0.9.1 for Python, SDK default settings, on separate cores.

Each cell is the median across repetitions, with the lowest and highest in brackets. Memory is resident memory (RSS) in MiB unless it says container, which also counts the page cache the kernel keeps for the server's files. CPU is percent of one core.

## Idle

| Server RSS | Server container | Server CPU | Managed backend RSS |
|---|---|---|---|
| 92 (92–92) | 71 (71–71) | 0.7% (0–1) | 58 (56–58) |

## 1. Consumer reading a 7 million record backlog

| Partitions | Consumer RSS peak | Consumer RSS median | Server RSS peak | Server container peak | Drain time (s) | Records/s | Missing |
|---|---|---|---|---|---|---|---|
| 2 | 73 | 72 | 150 (150–151) | 2,048 | 738 (421–1,134) | 9,481 (6,172–16,638) | 0 |
| 10 | 72 (72–73) | 72 (72–72) | 181 (180–187) | 2,048 | 757 (589–811) | 9,251 (8,632–11,884) | 0 |
| 50 | 72 (72–73) | 72 (72–72) | 196 (183–244) | 2,048 (2,048–2,048) | 267 (140–966) | 16,978 (7,247–17,821) | 2,237,500 (0–4,618,100) |

## 2. Producer for 30 minutes at 20,000 records/s, server paused for 60 s halfway

| Producer RSS before | Peak while paused | Peak after resume | Last 2 minutes | Growth before pause (MiB/h) | Growth after (MiB/h) | Failed writes | Missing |
|---|---|---|---|---|---|---|---|
| 62 (61–62) | 91 (90–91) | 94 (93–94) | 92 (73–94) | 11 (6–13) | 0 | 2 | 0 |

## 3. Server restart with 4 GiB on disk, producer and consumer connected

Steady before restart: server RSS 114 (113–117), CPU 11% (10–12).

| Restart | Server answers (s) | Producer writes again (s) | Consumer reads again (s) | Server RSS peak, first 60 s | Container peak | CPU peak |
|---|---|---|---|---|---|---|
| graceful | 9.5 (9–18) | 114 (95–125) | 26 (17–125) | 145 (145–151) | 2,049 (2,048–2,049) | 18% (16–24) |
| crash | 8.4 (8–13) | 89 (11–129) | 15 (11–39) | 244 (244–251) | 2,048 | 25% (21–42) |

Confirmed records missing after both restarts: 0.

Raw evidence for every repetition is in `rep-*/`: `samples.csv` (one row per second per process), `events.jsonl`, one JSON per test, `env.json` and the tail of the server log.
