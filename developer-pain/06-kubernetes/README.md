> **LaserData proof · Developer pain 06: Kubernetes ops & upgrades**  
> Blog: [Running a three-node Apache Iggy cluster on Kubernetes, and upgrading it without losing a message](https://claude.ai/artifact/RocbaVUwhtePPzsvfFCDQ5) · Doc: [Run Apache Iggy on Kubernetes](https://claude.ai/artifact/WrAtXuKKn8mXYnyJsaYdrM) · Posts: [ready-to-publish drafts](https://claude.ai/artifact/VHZp6Z8yooKaSaCES2bPnG#k8s)  
> Where this code goes: upstream/: pull request to apache/iggy to publish the Helm chart on Artifact Hub. reference/: LaserData three-node reference.  
> Status: not yet run against a live Apache Iggy server unless a checklist below says so. Nothing has been opened upstream.

# Apache Iggy on Kubernetes: LaserData contributions

- `upstream/`: a pull request for `apache/iggy` that publishes the Helm chart to
  `oci://ghcr.io/apache/iggy/charts/iggy` and lists it on Artifact Hub
  (`PULL_REQUEST.md` explains it).
- `reference/`: LaserData's three-node reference, with a scripted rolling
  upgrade and a zero-loss check (`reference/README.md`).
