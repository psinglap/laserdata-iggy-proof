# Footprint tests on Google Cloud VMs

For each test: one **server VM** running Laser Stack (LaserData's server and managed backend)
and one **client VM** running the Laser SDK tests against it over the zone's internal network,
the way a customer's app reaches its streaming server. All tests run at once, then every VM is
deleted.

| | Server VM | Client VM |
|---|---|---|
| Machine | `n2-standard-4`: 4 dedicated vCPU, 16 GB | `n2-standard-4` |
| Disk | 100 GB SSD persistent disk, normal mount (write barriers) | boot disk |
| Runs | Laser Stack, server on 2 cores with a 2 GiB limit (same as GitHub), own sampler | Laser SDK tests, client sampler, `docker pause/restart` on the server over SSH |

```bash
cd developer-pain/03-resource-footprint/gcp-vm
PROJECT=my-project ./run-gcp.sh                # all five tests, one repetition
PROJECT=my-project FOOTPRINT_SCALE=0.1 ./run-gcp.sh   # quick trial
PROJECT=my-project ZONE=us-central1-a ./run-gcp.sh
python ../footprint/merge.py results/sized-2g/incoming results/sized-2g
python ../footprint/summarize.py results/sized-2g && python ../footprint/charts.py results/sized-2g
```

Cost: two `n2-standard-4` VMs per test for about 45 minutes, roughly $0.20 per test in
`asia-south1`. The script deletes every VM it created when it finishes or fails.

Next step: point the client VM at a LaserData Cloud deployment on dedicated GCP hosts instead of
the server VM (`REMOTE_SERVER` / `LASER_CONNECTION_STRING`), once the account tier allows it.
