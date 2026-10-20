#!/usr/bin/env bash
# One Secret shared by all three releases: these values must be byte-identical
# on every node. From the apache/iggy chart README.
set -euo pipefail
NS="${NS:-iggy}"
kubectl get ns "$NS" >/dev/null 2>&1 || kubectl create ns "$NS"
JWT="$(head -c 32 /dev/urandom | base64)"
kubectl -n "$NS" create secret generic iggy-cluster-secrets \
  --from-literal=username=admin \
  --from-literal=password="$(head -c 24 /dev/urandom | base64)" \
  --from-literal=clusterSharedSecret="$(head -c 32 /dev/urandom | base64)" \
  --from-literal=jwtEncodingSecret="$JWT" \
  --from-literal=jwtDecodingSecret="$JWT" \
  --from-literal=encryptionKey="$(head -c 32 /dev/urandom | base64)"
