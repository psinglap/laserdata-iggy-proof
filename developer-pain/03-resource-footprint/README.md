> **LaserData proof · Developer pain 03: Memory / CPU**  
> Blog: [Consumers that grow to 6 GB, producers that leak, brokers that spike on restart: we measured LaserData on all three](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.resource-blog) · Doc: [Size memory and CPU for LaserData](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.resource-doc) · Posts: [ready-to-publish drafts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~resource)  
> Where this code goes: the LaserData use-case repository, after LaserData engineering has reviewed it.  
> Status: measured on GitHub Actions (3 repetitions) and on Google Cloud VMs (1 repetition). Results: [github/results/sized-2g](github/results/sized-2g/README.md) · [gcp-vm/results/sized-2g](gcp-vm/results/sized-2g/README.md) · shipped-settings evidence for review: [github/results/shipped](github/results/shipped/NOTE.md).

# LaserData footprint tests

Three tests that measure the memory and CPU of LaserData's server (Laser Streams) and of
Laser SDK clients in the situations engineers report most:

| Test | The problem it answers | What runs | Pass condition |
|---|---|---|---|
| `consumer_backlog.py` | A consumer that grows from 800 MB to 6 GB when partitions and lag go up | 7 million records (1 KiB) preloaded at 2, 10 and 50 partitions; one Laser SDK consumer reads them all from offset 0 | every record read once |
| `producer_pause.py` | A producer whose memory keeps climbing under sustained sends | Laser SDK producer, background mode, 8,000 records/s for 30 minutes; server frozen with `docker pause` for 60 s halfway; a consumer checks every record | no record missing unless the SDK reported it to the app |
| `restart.py` | A broker restart that spikes CPU and memory and stalls its neighbours | 4 GiB on disk; producer (5,000 records/s) and consumer connected; graceful restart, then `kill -9` and start | no confirmed record missing |

## Two setups, same tests

| Folder | Where | Client and server |
|---|---|---|
| [`github/`](github/README.md) | GitHub-hosted machines, one per test, all at once | Same machine, separate cores |
| [`gcp-vm/`](gcp-vm/README.md) | Google Cloud VMs (`n2-standard-4`), one pair per test | Separate VMs in one zone, like an app and its streaming server |

Both use the **sized-2g** server profile (`stack/sized-2g.yml`): Laser Stack's settings with the memory
pool at 512 MiB and the write-ahead log at its minimum (~128 MiB per partition), sized for the
2 GiB node. With the shipped budgets, a 50-partition write burst got the server killed on that node;
see `github/results/shipped/NOTE.md`.

## How it measures

- **Server:** Laser Streams and LaserData's managed backend, started with
  [Laser Stack](https://github.com/laserdata/laser-stack)'s own `scripts/up` at a pinned commit,
  with LaserData's shipped settings. `stack/limits.yml` is the only addition: it gives the server
  two dedicated cores and a 2 GiB memory limit, and puts the clients on the other cores.
- **Clients:** [Laser SDK](https://docs.laserdata.com/laser-sdk) for Python, SDK default settings.
  Record size (1 KiB) and batch size (100) are taken from LaserData's own benchmark suite.
- **Numbers:** `footprint/sampler.py` reads every process's resident memory and CPU from `/proc`
  and cgroup v2 once a second. The container total (which includes page cache) is recorded too.
- **Correctness:** every record carries a sequence number; missing or duplicate records are counted.

## Run it

**On GitHub, no Linux machine needed.** Fork this repository, open **Actions**, choose
**LaserData footprint**, then **Run workflow**. `scale = 0.1` is an 8-minute trial that publishes
nothing; `scale = 1` runs the full hour three times on fresh machines and commits `results/`.

**On Linux with Docker:**

```bash
pip install -r requirements.txt
FOOTPRINT_SCALE=0.1 ./run.sh      # 8-minute trial
./run.sh                          # full run, about 1 hour
python footprint/summarize.py results   # if you put several runs in results/rep-*/
```

**Against your own LaserData deployment:** set `LASER_CONNECTION_STRING` and run one test
directly, for example `python footprint/consumer_backlog.py preload --partitions 50`, then
`consume`. Client memory is still sampled locally; read server memory from the LaserData Console.

**With a coding agent:** see `AGENTS.md`.

## Things we found while building it

- GitHub-hosted runners mount their disks with `nobarrier`. LaserData's managed backend refuses
  to keep data on such a disk, so the workflow creates an ext4 image with write barriers
  (`stack/data-disk.yml`). Local runs on a normal disk don't need it.
- The test consumer is one Python process handling one record at a time, so it is CPU-bound at
  roughly 10,000–13,000 records/s. That limits the test client, not LaserData, so the results make
  no throughput claims.
- After a 60-second server freeze, a Laser SDK consumer raises `the live consumer stream ended`.
  The tests then build a new consumer in the same group, as a long-running service should, and
  record how often that was needed.

## Files

| File | Purpose |
|---|---|
| `run.sh` | Starts Laser Stack with the limits, runs the three tests, collects evidence |
| `stack/limits.yml` | Cores and memory limit for the server; the only change to Laser Stack |
| `stack/data-disk.yml` | CI only: data on a disk with write barriers |
| `footprint/sampler.py` | Per-second memory and CPU from the kernel |
| `footprint/consumer_backlog.py`, `producer_pause.py`, `restart.py` | The three tests |
| `footprint/summarize.py`, `charts.py` | Build `results/README.md`, `summary.json` and the charts |
| `../../.github/workflows/footprint.yml` | The GitHub workflow |

Built by the LaserData team.
