#!/usr/bin/env bash
# Upgrades a 3-node Apache Iggy cluster one node at a time while a client keeps
# writing, then checks that every acknowledged message is still readable.
#
#   NEW_TAG=0.9.1 CHART=./iggy/helm/charts/iggy ./rolling-upgrade.sh
#
# With hostNetwork the chart uses the Recreate strategy, so each node is down
# for the length of its restart. Two of three replicas keep quorum meanwhile.
set -euo pipefail
NS="${NS:-iggy}"
NEW_TAG="${NEW_TAG:?image tag to upgrade to}"
CHART="${CHART:?path or OCI reference of the iggy chart}"
SETTLE="${SETTLE:-60}"   # seconds to let the cluster settle after each node

# Helm 4 uses server-side apply; a Recreate strategy change needs --server-side=false
# (see the chart README). Harmless to omit on Helm 3.
HELM_FLAGS=()
helm version --short | grep -q '^v4' && HELM_FLAGS+=(--server-side=false)

kubectl -n "$NS" delete job iggy-loss-check --ignore-not-found
kubectl -n "$NS" create configmap iggy-loss-check --from-file=loss_check.py \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" apply -f loss-check-job.yaml
echo "Loss check running. Upgrading one node at a time to $NEW_TAG."
sleep 30   # let the writer reach a steady rate first

for i in 0 1 2; do
  echo "== node $i =="
  helm upgrade "iggy-n$i" "$CHART" -n "$NS" --reuse-values ${HELM_FLAGS[@]+"${HELM_FLAGS[@]}"} \
    --set "server.image.tag=$NEW_TAG"
  kubectl -n "$NS" rollout status "deploy/iggy-n$i" --timeout=10m
  echo "node $i ready (GET /ping); settling ${SETTLE}s before the next node"
  sleep "$SETTLE"
done

echo "All nodes on $NEW_TAG. Waiting for the loss check to finish."
kubectl -n "$NS" wait --for=condition=complete job/iggy-loss-check --timeout=30m
kubectl -n "$NS" logs job/iggy-loss-check | tail -n 20
