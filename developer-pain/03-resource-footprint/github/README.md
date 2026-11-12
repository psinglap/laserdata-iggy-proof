# Footprint tests on GitHub Actions

Each test runs on its own fresh GitHub-hosted Linux machine (4 shared vCPU, 16 GB), all at once.
The Laser Streams server and the Laser SDK clients share that machine, on separate cores.
Anyone can repeat this: fork the repository, open **Actions → LaserData footprint → Run workflow**.

- Workflow: [`.github/workflows/footprint.yml`](../../../.github/workflows/footprint.yml)
- Results: [`results/sized-2g/`](results/sized-2g/README.md) (server memory budgets sized for a 2 GiB node)

Good for: reproducible numbers anyone can check, client memory, a small single node.
Not for: production performance claims. The machines are shared, the disk needs a workaround
(see `../stack/data-disk.yml`), and client and server compete for the same host.
