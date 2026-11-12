# For coding agents (Claude Code, Cursor, Codex)

This folder measures the memory and CPU of LaserData's server (Laser Streams) and of Laser SDK
clients in three situations: a consumer catching up on a large backlog, a producer whose server
stops answering, and a server restart. Read `README.md` first.

## Run it for the user

- **On Google Cloud:** `PROJECT=<their project> gcp-vm/run-gcp.sh` (needs `gcloud` signed in);
  `FOOTPRINT_SCALE=0.1` for a quick trial. It deletes every VM it creates.

## Rules

- Change one thing at a time (partitions, record size, rate, memory limit) and say what changed.
- Never edit the numbers in `results/`. They are written by the scripts from the raw samples.
- A run where a test reports missing records is a failed run. Report it, do not publish it.

## Looking things up

Add the LaserData docs MCP server so you can check Laser SDK calls instead of guessing:

```
claude mcp add --transport http laserdata-docs https://laserdata-docs-mcp.fly.dev/mcp
```

Laser SDK docs: https://docs.laserdata.com/laser-sdk · Laser Stack: https://github.com/laserdata/laser-stack
