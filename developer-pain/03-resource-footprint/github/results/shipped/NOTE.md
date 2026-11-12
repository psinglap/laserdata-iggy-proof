# Read this first: shipped-settings run (internal evidence)

Run [43276020994](https://github.com/psinglap/laserdata-iggy-proof/actions/runs/43276020994),
Laser Stack exactly as shipped, server limited to 2 cores and 2 GiB.

- **Consumer backlog at 50 partitions, repetitions 1 and 2: the server was killed.** While 7 million
  records were written as fast as possible, server memory rose past the 2 GiB limit (memory pool
  budget 4 GiB, write-ahead log budget 268 MB per partition), the kernel killed it twice, and on
  restart it logged `discarding unpublished segment bytes beyond the durable WAL frontier`.
  The consumer then found 2.38 million of the 7 million records the producer had been told were
  stored. Evidence: `rep-*/samples.csv`, `server-tail-backlog-50.log`, `consumer_backlog-50p.json`.
  Repetition 3 survived (server peak 565 MiB). For LaserData engineering to review.
- **Producer test: not valid.** It sent 20,000 records/s, faster than the single Python checking
  consumer can read, with 10-minute retention, so records expired before they were checked.
  Fixed afterwards (8,000 records/s, no expiry). Repetition 1 also hit the job time limit.
- Consumer backlog at 2 and 10 partitions, idle and restart results are valid.

The published numbers come from `../sized-2g/`: the same tests with the two memory budgets
shrunk to fit a 2 GiB node (`stack/sized-2g.yml`).
