> **LaserData proof · Developer pain 09: Cross-AZ & storage cost**  
> Blog: [What it costs to run Apache Iggy on AWS, with every assumption showing](https://claude.ai/artifact/QMTD8Rh8NbJP26i5hLiwSd) · Doc: [Estimate the cost of running Apache Iggy](https://claude.ai/artifact/XMsi1aDRuodoKCaLuDVutV) · Posts: [ready-to-publish drafts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#cost)  
> Where this code goes: LaserData repo: an open-source calculator.  
> Status: not yet run against a live Apache Iggy server unless a checklist below says so. Nothing has been opened upstream.

# Apache Iggy TCO calculator

A one-page calculator, from LaserData, that estimates the monthly AWS cost of running an
[Apache Iggy](https://iggy.apache.org) cluster for a given workload. Open `index.html` in a browser;
there is nothing to install.

## What it counts

| Line item | How it is calculated |
|---|---|
| Compute | nodes × instance $/hour × 730 hours |
| Storage | local NVMe is included in the instance price; EBS-only instances add gp3 for `stored GB ÷ (1 − headroom)` |
| Cross-zone traffic | with 3 zones: one copy per extra replica, plus the share of producer and consumer traffic that reaches a primary in another zone |

Nodes = the largest of: the replica count, the nodes needed to hold the retained data, and
`write rate ÷ measured per-node write rate`.

## What it does not assume

- **No throughput multiplier.** The per-node write rate is yours to measure, for example with the
  [OpenMessaging Benchmark driver for Apache Iggy](../omb-driver-iggy). Until you enter it, the
  calculator says that throughput was not checked.
- **No tiered storage.** Apache Iggy keeps all retained data on node disks today. The dashed
  "tiered reference" line shows what a design with object-storage offload would cost, so you can see
  when long retention starts to matter. It is a reference, not an Apache Iggy feature.
- **List prices.** AWS us-east-1, Linux, on-demand, checked 19 October 2026. Replace them with your
  own discounted prices under "Prices and assumptions".
- **Placement.** The model treats the cluster as nodes that together hold `replicas` copies of every
  message. Check the Apache Iggy clustering docs for your version before sizing beyond one replica set.

## Files

- `model.js`: the cost model, a pure function with no dependencies
- `test.js`: model tests, run with `node test.js`
- `calc.fragment.html`: the calculator UI; `build_index.py` inlines it into `index.html`

## Running it for real

LaserData runs Apache Iggy as a managed service or in your own AWS or GCP account. If you want this
estimate checked against a benchmark on your workload, the LaserData team will run it with you.

Maintained by LaserData. I work at LaserData, which builds and runs Apache Iggy.
