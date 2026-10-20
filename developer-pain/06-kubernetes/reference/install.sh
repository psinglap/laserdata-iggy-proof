#!/usr/bin/env bash
# Installs one release per node. NODES must list the Kubernetes node names in
# replicaId order, and their IPs must match values-3-node.yaml.
#   NODES="node-a node-b node-c" CHART=./iggy/helm/charts/iggy ./install.sh
set -euo pipefail
NS="${NS:-iggy}"
CHART="${CHART:?path to helm/charts/iggy, or oci://ghcr.io/apache/iggy/charts/iggy once published}"
read -r -a NODE <<<"${NODES:?space-separated node names}"
for i in 0 1 2; do
  helm upgrade --install "iggy-n$i" "$CHART" -n "$NS" \
    -f values-3-node.yaml \
    --set "server.cluster.selfReplicaId=$i" \
    --set "server.nodeSelector.kubernetes\.io/hostname=${NODE[$i]}"
done
for i in 0 1 2; do kubectl -n "$NS" rollout status "deploy/iggy-n$i" --timeout=5m; done
