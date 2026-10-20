# LaserData proof for Apache Iggy

Code behind LaserData's Apache Iggy blogs, docs and use-case write-ups, in one place.
LaserData builds and runs [Apache Iggy](https://iggy.apache.org), the open-source message streaming platform written in Rust.

- **developer-pain/** has one folder per developer problem in the market map's section 02: the kit, example or open-source contribution that proves LaserData's answer.
- **use-cases/** has one runnable demo per use case in section 04.

Each folder's README links its blog, doc and posts, says where the code is meant to go (a LaserData repo, or a pull request to another project), and what has not been tested yet.

## Developer pain

| # | Problem | Code | Blog, doc and posts |
|---|---|---|---|
| 01 | Latency & p99 spikes | [developer-pain/01-latency](developer-pain/01-latency) | [Blog](https://claude.ai/artifact/AihwxLB4tu9oYCT2GURUzD) · [Doc](https://claude.ai/artifact/3bx8AbaY3BQWvL2TDC4rQj) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#latency) |
| 02 | Local dev / testing heaviness | [developer-pain/02-local-testing](developer-pain/02-local-testing) | [Blog](https://claude.ai/artifact/S7vUqFNAw6BEFAH55ZWCrQ) · [Doc](https://claude.ai/artifact/DcXQfxqniYrh4zWKTf9pkz) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#devx) |
| 03 | Memory / CPU / JVM / GC | [developer-pain/03-resource-footprint](developer-pain/03-resource-footprint) | [Blog](https://claude.ai/artifact/NTugtAhsMsQAnXFCLforrB) · [Doc](https://claude.ai/artifact/16tgJNuUiuN6CtMt5yKiLq) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#resource) |
| 04 | Partition limits & hot partitions | [developer-pain/04-partitions](developer-pain/04-partitions) | [Blog](https://claude.ai/artifact/Mp6DiKAoiMLTroUWh8VDuV) · [Doc](https://claude.ai/artifact/XnKggTfVSJN8v1krnFPqRH) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#partitions) |
| 05 | TLS / SASL / auth setup | [developer-pain/05-auth](developer-pain/05-auth) | [Blog](https://claude.ai/artifact/6U58MVy4i1ozNW4xZmcRpL) · [Doc](https://claude.ai/artifact/REMoG5hfUcyW9SM1uGCr6P) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#auth) |
| 06 | Kubernetes ops & upgrades | [developer-pain/06-kubernetes](developer-pain/06-kubernetes) | [Blog](https://claude.ai/artifact/RocbaVUwhtePPzsvfFCDQ5) · [Doc](https://claude.ai/artifact/WrAtXuKKn8mXYnyJsaYdrM) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#k8s) |
| 07 | Consumer-group rebalancing & stalls | [developer-pain/07-rebalancing](developer-pain/07-rebalancing) | [Blog](https://claude.ai/artifact/7pcPmhKyZC9Y1EBWqxa2ar) · [Doc](https://claude.ai/artifact/Stm1Vo6iaRreBA8LrrBjCh) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#rebalance) |
| 08 | Lag & observability | [developer-pain/08-consumer-lag](developer-pain/08-consumer-lag) | [Blog](https://claude.ai/artifact/W8pKGcPYFD3kVxe9jBR9Kz) · [Doc](https://claude.ai/artifact/MrTWgMAbMGTCKoh3hi9GPh) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#lag) |
| 09 | Cross-AZ & storage cost | [developer-pain/09-cost](developer-pain/09-cost) | [Blog](https://claude.ai/artifact/QMTD8Rh8NbJP26i5hLiwSd) · [Doc](https://claude.ai/artifact/XMsi1aDRuodoKCaLuDVutV) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#cost) |
| 10 | Client library maintenance & SDK parity | [developer-pain/10-sdks](developer-pain/10-sdks) | [Blog](https://claude.ai/artifact/7sZ5GGszNS2LgNoDHmUxx6) · [Doc](https://claude.ai/artifact/X6Cct1aEmpUBXkfPH9EEPt) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#sdk) |
| 11 | Exactly-once / transactions | [developer-pain/11-delivery-guarantees](developer-pain/11-delivery-guarantees) | [Blog](https://claude.ai/artifact/AWTyiQyE39wfmukJRK9jqH) · [Doc](https://claude.ai/artifact/Ve5w5gKoPr81vzNSt2F9xJ) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#eos) |
| 12 | Schema registry & serialization | [developer-pain/12-schema-registry](developer-pain/12-schema-registry) | [Blog](https://claude.ai/artifact/DdkNgaGmUruHPKYqyDt3UF) · [Doc](https://claude.ai/artifact/CP2VuYPXKnKzmD2H7Tr4y5) · [Posts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#schema) |

## Use cases

| Use case | Code | Write-up |
|---|---|---|
| Real-time price distribution | [use-cases/iggy-market-data-fanout](use-cases/iggy-market-data-fanout) | [Write-up](https://claude.ai/artifact/Uev8ENpRDvbDGD9qmJoJu5) |
| Live scores, odds and reactions | [use-cases/iggy-live-event-fanout](use-cases/iggy-live-event-fanout) | [Write-up](https://claude.ai/artifact/DVVsz2FncTn7woHgGdDGKv) |
| Bids, wins and ad budgets | [use-cases/iggy-rtb-bidstream](use-cases/iggy-rtb-bidstream) | [Write-up](https://claude.ai/artifact/VFMpGjoHSatq53biam3LJB) |
| Courier location, dispatch and ETA | [use-cases/iggy-rider-eta](use-cases/iggy-rider-eta) | [Write-up](https://claude.ai/artifact/NqdmKpAxWFs24DFg9e14zm) |
| Live inventory and pricing | [use-cases/iggy-inventory-pricing](use-cases/iggy-inventory-pricing) | [Write-up](https://claude.ai/artifact/PLi9aaFNcPw9tLyd6Dt29Q) |
| Engagement events and real-time triggers | [use-cases/iggy-engagement-triggers](use-cases/iggy-engagement-triggers) | [Write-up](https://claude.ai/artifact/N6c54bVAphEFDsE63ahzad) |
| Telemetry and alerting hot tier | [use-cases/iggy-telemetry-hot-tier](use-cases/iggy-telemetry-hot-tier) | [Write-up](https://claude.ai/artifact/D6ZC5zzQEnxGuSstwJFq1P) |
| An event bus for AI voice and chat agents | [use-cases/iggy-agent-bus](use-cases/iggy-agent-bus) | [Write-up](https://claude.ai/artifact/Lc9V5XNzXEPWQLuBMk8XAh) |
| Game telemetry and live-ops | [use-cases/iggy-game-telemetry](use-cases/iggy-game-telemetry) | [Write-up](https://claude.ai/artifact/CJwxBueEkXSQB1qcDAkQN6) |
| Fraud and risk features | [use-cases/iggy-fraud-features](use-cases/iggy-fraud-features) | [Write-up](https://claude.ai/artifact/Nsc3XPg3aMSXSQvsRYdceU) |
| Usage metering and quotas | [use-cases/iggy-usage-metering](use-cases/iggy-usage-metering) | [Write-up](https://claude.ai/artifact/DQabUVUxqoh4ruz7PBEtf9) |
| Edge store-and-forward | [use-cases/iggy-edge-forward](use-cases/iggy-edge-forward) | [Write-up](https://claude.ai/artifact/JjRJYtduzFnRxyawUbw8iJ) |
| An order and trade event log | [use-cases/iggy-order-event-log](use-cases/iggy-order-event-log) | [Write-up](https://claude.ai/artifact/S3LGejBGm4yzn2wJhtcG34) |

## Status

- Nothing here has been run against a live Apache Iggy server yet; each folder lists what was checked (compile, import, unit tests).
- Benchmarks have no published numbers until they are run on real machines.
- No pull requests have been opened in other projects.

The blog, doc and write-up links are private review pages; ask the owner for access.
