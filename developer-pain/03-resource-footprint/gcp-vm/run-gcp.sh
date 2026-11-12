#!/usr/bin/env bash
# The footprint tests on Google Cloud, from your laptop: for each test, one server VM running
# Laser Stack and one client VM running the Laser SDK tests against it over the zone's internal
# network. All tests run at once. Evidence comes back to results/<profile>/, then every VM is deleted.
#
#   ./run-gcp.sh                                   all five tests, one repetition
#   TESTS="restart" ./run-gcp.sh                   one test
#   PROJECT=my-project ZONE=us-central1-a ./run-gcp.sh
#
# Needs: gcloud signed in to a project where you can create VMs.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
PROJECT=${PROJECT:-gtm-tools-513915}; ZONE=${ZONE:-asia-south1-a}
MACHINE=${MACHINE:-n2-standard-4}       # 4 dedicated vCPU, 16 GB, for each VM
PROFILE=${PROFILE:-sized-2g}; SCALE=${FOOTPRINT_SCALE:-1}; REP=${REP:-1}
TESTS=${TESTS:-backlog-2 backlog-10 backlog-50 producer restart}
REF=${REF:-$(git -C "$HERE" rev-parse HEAD)}
RUN=fp$(date +%m%d%H%M)
OUT="$HERE/results/$PROFILE"; IN="$OUT/incoming"; mkdir -p "$IN"
g() { gcloud --project "$PROJECT" --quiet "$@"; }
sshv() { local vm=$1; shift; g compute ssh "$vm" --zone "$ZONE" --command "$*"; }

VMS=()
cleanup() { [ ${#VMS[@]} -eq 0 ] || g compute instances delete "${VMS[@]}" --zone "$ZONE" >/dev/null 2>&1 || true; echo "deleted ${VMS[*]:-nothing left}"; }
trap cleanup EXIT

echo "== creating VMs in $ZONE ($MACHINE)"
for t in $TESTS; do
  s=$RUN-$t-s; c=$RUN-$t-c; VMS+=("$s" "$c")
  g compute instances create "$s" --zone "$ZONE" --machine-type "$MACHINE" \
    --image-family ubuntu-2404-lts-amd64 --image-project ubuntu-os-cloud --boot-disk-size 30GB \
    --create-disk=name=$s-data,device-name=data,size=100GB,type=pd-ssd,auto-delete=yes \
    --metadata-from-file startup-script="$HERE/setup-vm.sh" --metadata fp-ref="$REF" \
    --labels purpose=laserdata-footprint >/dev/null &
  g compute instances create "$c" --zone "$ZONE" --machine-type "$MACHINE" \
    --image-family ubuntu-2404-lts-amd64 --image-project ubuntu-os-cloud --boot-disk-size 30GB \
    --metadata-from-file startup-script="$HERE/setup-vm.sh" --metadata fp-ref="$REF" \
    --labels purpose=laserdata-footprint >/dev/null &
done
wait
echo "== waiting for setup"
for vm in "${VMS[@]}"; do
  until sshv "$vm" "test -f /opt/fp/ready" >/dev/null 2>&1; do sleep 10; done; echo "ready: $vm"
done

start_test() {
  local t=$1 s=$RUN-$1-s c=$RUN-$1-c ip
  ip=$(g compute instances describe "$s" --zone "$ZONE" --format='value(networkInterfaces[0].networkIP)')
  # Let the client VM run docker pause/restart on the server VM over SSH.
  sshv "$c" "test -f ~/.ssh/id_ed25519 || ssh-keygen -q -t ed25519 -N '' -f ~/.ssh/id_ed25519"
  local key; key=$(sshv "$c" "cat ~/.ssh/id_ed25519.pub")
  sshv "$s" "mkdir -p ~/.ssh && echo '$key' >> ~/.ssh/authorized_keys"
  sshv "$s" "bash /opt/fp/repo/developer-pain/03-resource-footprint/gcp-vm/server-up.sh $PROFILE"
  local tests=$t; [ "$t" = restart ] && tests="idle restart"
  sshv "$c" "cd /opt/fp && nohup setsid bash -c 'env REMOTE_SERVER=$ip PROFILE=$PROFILE TESTS=\"$tests\" FOOTPRINT_SCALE=$SCALE \
    CLIENT_CPUS=0-3 PYTHON=/opt/fp/venv/bin/python FOOTPRINT_OUT=/opt/fp/out \
    bash repo/developer-pain/03-resource-footprint/run.sh > /opt/fp/run.log 2>&1; touch /opt/fp/done' > /dev/null 2>&1 < /dev/null &"
  echo "started $t (server $ip)"
}
for t in $TESTS; do start_test "$t" & done
wait

echo "== running; checking every minute"
for t in $TESTS; do
  c=$RUN-$t-c; s=$RUN-$t-s
  until sshv "$c" "test -f /opt/fp/done" >/dev/null 2>&1; do sleep 60; done
  sshv "$s" "sudo touch /opt/fp/server-out/stop; sleep 3; sudo tar czf /tmp/server.tgz -C /opt/fp/server-out ."
  sshv "$c" "tail -5 /opt/fp/run.log; tar czf /tmp/client.tgz -C /opt/fp/out ."
  d=$(mktemp -d)
  g compute scp --zone "$ZONE" "$s:/tmp/server.tgz" "$d/" >/dev/null
  g compute scp --zone "$ZONE" "$c:/tmp/client.tgz" "$d/" >/dev/null
  mkdir -p "$d/s" "$d/c" && tar xzf "$d/server.tgz" -C "$d/s" && tar xzf "$d/client.tgz" -C "$d/c"
  python3 "$HERE/collect.py" "$d/c" "$d/s" "$IN/rep-$REP-$t"
  g compute instances delete "$s" "$c" --zone "$ZONE" >/dev/null 2>&1 || true
  echo "finished $t, VMs deleted"
done
VMS=()
echo "== evidence in $IN"
