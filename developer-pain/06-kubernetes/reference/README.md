# Apache Iggy on Kubernetes: a three-node reference, from LaserData

A tested-by-script way to run a three-node Apache Iggy 0.9 cluster with the
official Helm chart, and to upgrade it one node at a time while proving that no
acknowledged message is lost.

| File | What it does |
|---|---|
| `values-3-node.yaml` | Values for the official chart (0.6.0, in `apache/iggy` under `helm/charts/iggy`): pinned 0.9.0 image, node-local storage, replica auth, encryption at rest |
| `create-secrets.sh` | The one Secret every node must share: root user, cluster PSK, JWT key, encryption key |
| `install.sh` | One Helm release per node, each pinned to its node |
| `rolling-upgrade.sh` | Upgrades node 0, 1, 2 in turn while `loss_check.py` writes and later verifies |
| `loss_check.py` + `loss-check-job.yaml` | Writes numbered messages through the upgrade, reads them back, fails if any acknowledged number is missing |

## How the chart runs a cluster

The chart models a cluster as **one release per node**, not as a StatefulSet.
Each server binds its replica listener to the literal IP in the shared roster,
so each pod uses `hostNetwork` and is pinned to a node whose IP matches.
Consequences you should plan for:

- Storage must be node-local (`local-path`, TopoLVM or local PVs). A replica
  that starts on an empty volume rejoins by copying everything from its peers.
- With `hostNetwork` the chart uses the `Recreate` strategy, so each node is
  down while it restarts. Two of three replicas keep quorum, so roll one at a time.
- `server.replicaCount > 1` is refused by the chart. Cluster size is a roster change.
- Nodes need Linux 5.19 or newer for `io_uring`; the chart already sets the
  unconfined seccomp profile and `IPC_LOCK`.

## Run it

```bash
git clone https://github.com/apache/iggy.git          # the chart is not in a Helm repo yet
# edit the three ip values in values-3-node.yaml to your node IPs
./create-secrets.sh
NODES="node-a node-b node-c" CHART=./iggy/helm/charts/iggy ./install.sh
NEW_TAG=<next release> CHART=./iggy/helm/charts/iggy ./rolling-upgrade.sh
```

The last line of the loss-check log is a JSON verdict:
`acknowledged`, `read_back`, `missing_acknowledged`, `duplicates`, `verdict`.
Duplicates are expected after retried sends (at-least-once); missing
acknowledged messages are a failure.

## Status

Scripts pass `bash -n`, YAML parses, and `loss_check.py` compiles with its SDK
calls checked against `apache-iggy` 0.9.0. The full run on a real three-node
cluster has not been done yet; results will be published here when it has.

Maintained by LaserData, which builds and runs Apache Iggy.
