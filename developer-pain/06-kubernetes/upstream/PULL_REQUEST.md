# ci(helm): publish the iggy chart to an OCI registry and list it on Artifact Hub

Today the only way to install the Helm chart is to clone the whole repository
(`helm install iggy ./helm/charts/iggy`). That keeps the chart out of the places
Kubernetes users look for software: `helm search hub`, Artifact Hub, and
`helm install oci://...` in a GitOps repo, where a git clone is not an option.

This adds a manual workflow, modelled on `publish.yml` (workflow_dispatch, an
explicit commit, dry run by default), that:

1. runs the existing `scripts/ci/test-helm.sh validate`,
2. packages `helm/charts/iggy`,
3. refuses to overwrite an existing chart version,
4. pushes it to `oci://ghcr.io/apache/iggy/charts/iggy` with the workflow's
   `GITHUB_TOKEN` (no new secrets),
5. pushes `artifacthub-repo.yml` as the `artifacthub.io` tag, which is how
   Artifact Hub reads ownership metadata for OCI repositories.

It also adds Artifact Hub annotations to Chart.yaml and an install-from-OCI
section to the chart README (see `CHART_YAML_CHANGES.md`).

## Decisions for the PMC

- **Registry.** GHCR keeps the chart under the apache/iggy GitHub namespace and
  needs no new credentials. Docker Hub is the alternative, but `apache/iggy` is
  already the server image there, and a chart pushed as `iggy` would collide
  with it.
- **Release policy.** Charts would follow the same vote as other convenience
  binaries. The workflow publishes nothing unless someone runs it with
  `dry_run: false`.
- **Artifact Hub.** After the first push, a PMC member adds the repository on
  artifacthub.io (kind Helm, URL `oci://ghcr.io/apache/iggy/charts/iggy`) and
  pastes the repository ID into `artifacthub-repo.yml`.

- **Actions allowlist.** `oras-project/setup-oras` is not used elsewhere in
  this repo. If it is not on the ASF Actions allowlist, the step can install the
  oras release binary with a pinned checksum instead, the way
  `setup-helm-tools` installs Helm.

## Not in this PR

`appVersion` is still `0.9.0-edge.6`. Moving it to `0.9.0` also needs the
`server.encryption.configVersion: auto` logic to pick `flat` for tags after
`0.9.0-edge.7` instead of failing. Happy to send that as a follow-up.

## Testing

- [ ] Workflow run with `dry_run: true` on a fork
- [ ] Workflow run with `dry_run: false` on a fork, pushing to the fork's GHCR
- [ ] `helm install` from the pushed OCI reference on kind
- [ ] Artifact Hub picks up the repository and metadata

Disclosure: I work at LaserData, which builds and runs Apache Iggy.
