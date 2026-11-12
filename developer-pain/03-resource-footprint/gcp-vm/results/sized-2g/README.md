# Footprint results

Measured 2026-11-12 with `gcp-vm/run-gcp.sh` on Google Cloud (`asia-south1-a`): for each test a server VM and a separate client VM, both `n2-standard-4`, 1 repetitions, each on a fresh GitHub-hosted machine (4 vCPU Intel(R) Xeon(R) CPU @ 2.80GHz, 15 GiB, kernel 7.0.0-1017-gcp).

Server: Laser Streams `server 0.10.1-ld` (`laserdatainc/iggy-server@sha256:348e27e6dca7a1a1b500e89d028dccc61ce7834ea9d76f4e621b3da6ed00a941`), from Laser Stack commit `46dd016`, LaserData's shipped settings, limited to 2g of memory and 2 dedicated cores. Clients: Laser SDK 0.9.1 for Python, SDK default settings, on separate cores.

Each cell is the median across repetitions, with the lowest and highest in brackets. Memory is resident memory (RSS) in MiB unless it says container, which also counts the page cache the kernel keeps for the server's files. CPU is percent of one core.

## Idle

| Server RSS | Server container | Server CPU | Managed backend RSS |
|---|---|---|---|
| 120 | 101 | 1.2% | 52 |

## 1. Consumer reading a 7 million record backlog

| Partitions | Consumer RSS peak | Consumer RSS median | Server RSS peak | Server container peak | Drain time (s) | Records/s | Missing |
|---|---|---|---|---|---|---|---|
| 2 | 72 | 71 | 185 | 2,048 | 900 | 7,782 | 0 |
| 10 | 71 | 71 | 218 | 2,048 | 904 | 7,739 | 0 |
| 50 | 67 | 63 | 272 | 2,048 | 927 | 7,550 | 0 |

## 2. Producer for 30 minutes at 8,000 records/s, server paused for 60 s halfway

| Producer RSS before | Peak while paused | Peak after resume | Last 2 minutes | Growth before pause (MiB/h) | Growth after (MiB/h) | Failed writes | Missing |
|---|---|---|---|---|---|---|---|
| 52 | 83 | 84 | 72 | 15 | 0 | 2 | 0 |

## 3. Server restart with 4 GiB on disk, producer and consumer connected

Steady before restart: server RSS 144, CPU 18%.

| Restart | Server answers (s) | Producer writes again (s) | Consumer reads again (s) | Server RSS peak, first 60 s | Container peak | CPU peak |
|---|---|---|---|---|---|---|
| graceful | 12 | 22 | 22 | 198 | 2,048 | 46% |
| crash | 7.7 | 10 | 17 | 171 | 1,869 | 77% |

Confirmed records missing after both restarts: 0.

Raw evidence for every repetition is in `rep-*/`: `samples.csv` (one row per second per process), `events.jsonl`, one JSON per test, `env.json` and the tail of the server log.
