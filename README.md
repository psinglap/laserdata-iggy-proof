# LaserData proof for Apache Iggy

Code behind LaserData's Apache Iggy blogs, docs and use-case write-ups, in one place.
LaserData builds and runs [Apache Iggy](https://iggy.apache.org), the open-source message streaming platform written in Rust.

- **developer-pain/** has one folder per developer problem in the market map's section 02: the kit, example or open-source contribution that proves LaserData's answer.
- **use-cases/** has one runnable demo per use case in section 04.

Each folder's README links its blog, doc and posts, says where the code is meant to go (a LaserData repo, or a pull request to another project), and what has not been tested yet.

## Developer pain

| # | Problem | Code | Blog, doc and posts |
|---|---|---|---|
| 01 | Latency & p99 spikes | [developer-pain/01-latency](developer-pain/01-latency) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.latency-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.latency-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~latency) |
| 02 | Local dev / testing heaviness | [developer-pain/02-local-testing](developer-pain/02-local-testing) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.devx-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.devx-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~devx) |
| 03 | Memory / CPU / JVM / GC | [developer-pain/03-resource-footprint](developer-pain/03-resource-footprint) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.resource-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.resource-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~resource) |
| 04 | Partition limits & hot partitions | [developer-pain/04-partitions](developer-pain/04-partitions) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.partitions-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.partitions-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~partitions) |
| 05 | TLS / SASL / auth setup | [developer-pain/05-auth](developer-pain/05-auth) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.auth-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.auth-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~auth) |
| 06 | Kubernetes ops & upgrades | [developer-pain/06-kubernetes](developer-pain/06-kubernetes) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.k8s-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.k8s-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~k8s) |
| 07 | Consumer-group rebalancing & stalls | [developer-pain/07-rebalancing](developer-pain/07-rebalancing) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.rebalance-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.rebalance-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~rebalance) |
| 08 | Lag & observability | [developer-pain/08-consumer-lag](developer-pain/08-consumer-lag) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.lag-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.lag-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~lag) |
| 09 | Cross-AZ & storage cost | [developer-pain/09-cost](developer-pain/09-cost) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.cost-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.cost-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~cost) |
| 10 | Client library maintenance & SDK parity | [developer-pain/10-sdks](developer-pain/10-sdks) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.sdk-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.sdk-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~sdk) |
| 11 | Exactly-once / transactions | [developer-pain/11-delivery-guarantees](developer-pain/11-delivery-guarantees) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.eos-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.eos-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~eos) |
| 12 | Schema registry & serialization | [developer-pain/12-schema-registry](developer-pain/12-schema-registry) | [Blog](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.schema-blog) · [Doc](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.schema-doc) · [Posts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~schema) |

## Use cases

| Use case | Code | Write-up |
|---|---|---|
| Real-time price distribution | [use-cases/iggy-market-data-fanout](use-cases/iggy-market-data-fanout) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-market-data-fanout) |
| Live scores, odds and reactions | [use-cases/iggy-live-event-fanout](use-cases/iggy-live-event-fanout) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-live-events) |
| Bids, wins and ad budgets | [use-cases/iggy-rtb-bidstream](use-cases/iggy-rtb-bidstream) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-ad-bidding) |
| Courier location, dispatch and ETA | [use-cases/iggy-rider-eta](use-cases/iggy-rider-eta) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-courier-location) |
| Live inventory and pricing | [use-cases/iggy-inventory-pricing](use-cases/iggy-inventory-pricing) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-inventory-pricing) |
| Engagement events and real-time triggers | [use-cases/iggy-engagement-triggers](use-cases/iggy-engagement-triggers) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-engagement-triggers) |
| Telemetry and alerting hot tier | [use-cases/iggy-telemetry-hot-tier](use-cases/iggy-telemetry-hot-tier) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-telemetry-hot-tier) |
| An event bus for AI voice and chat agents | [use-cases/iggy-agent-bus](use-cases/iggy-agent-bus) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-agent-bus) |
| Game telemetry and live-ops | [use-cases/iggy-game-telemetry](use-cases/iggy-game-telemetry) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-game-telemetry) |
| Fraud and risk features | [use-cases/iggy-fraud-features](use-cases/iggy-fraud-features) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-fraud-features) |
| Usage metering and quotas | [use-cases/iggy-usage-metering](use-cases/iggy-usage-metering) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-usage-metering) |
| Edge store-and-forward | [use-cases/iggy-edge-forward](use-cases/iggy-edge-forward) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-edge-forward) |
| An order and trade event log | [use-cases/iggy-order-event-log](use-cases/iggy-order-event-log) | [Write-up](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.usecase-order-event-log) |

## Status

- Nothing here has been run against a live Apache Iggy server yet; each folder lists what was checked (compile, import, unit tests).
- Benchmarks have no published numbers until they are run on real machines.
- No pull requests have been opened in other projects.

The blog, doc and write-up links open pages in the LaserData market map.
