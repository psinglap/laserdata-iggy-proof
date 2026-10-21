> **LaserData proof · Developer pain 10: Client library maintenance & SDK parity**  
> Blog: [Building a Node.js service on Apache Iggy](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.sdk-blog) · Doc: [Apache Iggy SDKs: pick one and connect in 5 minutes](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.sdk-doc) · Posts: [ready-to-publish drafts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~sdk)  
> Where this code goes: LaserData repo: a Node.js quickstart.  
> Status: not yet run against a live Apache Iggy server unless a checklist below says so. Nothing has been opened upstream.

# iggy-node-quickstart

A small Node.js service on [Apache Iggy](https://iggy.apache.org), from LaserData.
It shows the three things most services need on day one:

- **A keyed producer.** Every event for one customer goes to the same partition, so it stays in order.
- **A consumer group.** The server assigns partitions. Start a second consumer and the work is split between them.
- **A clean shutdown.** On `SIGINT` or `SIGTERM` the consumer finishes its batch, leaves the group so its partitions move right away, and closes the connection.

Uses the official Apache Iggy Node.js SDK, [`apache-iggy`](https://www.npmjs.com/package/apache-iggy) 0.10.0, against server 0.9.0.

## Run it

```bash
docker compose up -d          # Apache Iggy 0.9.0 on localhost:8090
npm install
npm run build
npm run consumer              # terminal 1
npm run consumer              # terminal 2: watch the partitions split
npm run producer              # terminal 3
```

Set `IGGY_CONNECTION_STRING` to point somewhere else, for example
`iggy+tcp://iggy:iggy@my-cluster:8090`.

## What's in `src/`

| File | What it does |
| --- | --- |
| `config.ts` | Names, the connection string, and `ensureTopology()`, which creates the stream and topic if they don't exist |
| `producer.ts` | Sends 30 order events with `Partitioning.MessageKey(customerId)` and an `event-type` header |
| `consumer.ts` | Joins the `billing` group, polls with `PollingStrategy.Next` and auto-commit, and leaves the group on shutdown |

## Things to know

- Delivery is at-least-once. A consumer that restarts can see a message again, so keep the handler idempotent.
- `docker-compose.yml` adds `SYS_NICE`, an unconfined seccomp profile and unlimited `memlock`, because the server uses `io_uring`.
- Status: type-checked with TypeScript 5.6 against `apache-iggy` 0.10.0. Not yet run end to end against a live server.

## Running Apache Iggy in production

LaserData builds and runs Apache Iggy as a managed service, or in your own AWS or GCP account: https://laserdata.com

Licensed under Apache 2.0.
