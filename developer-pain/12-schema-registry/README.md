> **LaserData proof · Developer pain 12: Schema registry & serialization**  
> Blog: [Versioned event schemas on Apache Iggy, with the schema id in a header](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.schema-blog) · Doc: [Use a schema registry with Apache Iggy](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.schema-doc) · Posts: [ready-to-publish drafts](https://claude.ai/artifact/A674Dn6Tu3dXoLFVokeLwC#p.posts~schema)  
> Where this code goes: LaserData repo: Apache Iggy with Apicurio Registry.  
> Status: not yet run against a live Apache Iggy server unless a checklist below says so. Nothing has been opened upstream.

# iggy-schema-registry-example

Typed, versioned events on [Apache Iggy](https://iggy.apache.org) with an external schema registry, from LaserData.

Apache Iggy doesn't include a schema registry, and it doesn't need one built in. Each Iggy message carries typed
user headers, so a producer can register its schema in any registry and put the schema id in a header. Consumers
read the header, fetch that exact schema once, and decode. This repo does it with
[Apicurio Registry](https://www.apicur.io/registry/) 3.3 and Avro.

```
producer ──register schema──▶ Apicurio ──globalId──▶ producer
producer ──payload: Avro bytes, header schema-id=<globalId>──▶ Apache Iggy topic shop/orders
consumer ◀── message ── Apache Iggy;  consumer ──GET /ids/globalIds/<id> (cached)──▶ Apicurio
```

## Run it

```bash
docker compose up -d                       # Apache Iggy 0.9.0 and Apicurio Registry 3.3.3
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python -m app.consumer                     # terminal 1
python -m app.producer v1                  # terminal 2: events written with schema v1
python -m app.producer v2                  # events written with schema v2
python -m app.evolve                       # v2 accepted, breaking v3 refused
```

The consumer is written against `order_v2.avsc`. Events written with v1 decode fine: Avro fills the missing
`currency` field with its default, `"USD"`.

## Files

| File | What it does |
| --- | --- |
| `app/registry.py` | Registers a schema (`POST /groups/shop/artifacts?ifExists=FIND_OR_CREATE_VERSION`), sets a `BACKWARD` compatibility rule, fetches schemas by `globalId` with a cache |
| `app/codec.py` | Avro encode/decode with `fastavro`; puts `schema-id` and `content-type` in the Iggy user headers |
| `app/producer.py` | Registers the chosen schema version and sends 5 keyed events |
| `app/consumer.py` | Joins the `analytics` consumer group, reads `schema-id`, decodes with writer and reader schemas |
| `app/evolve.py` | Shows the registry accepting a compatible change and refusing a breaking one |
| `schemas/` | `v1`, `v2` (adds `currency` with a default), `v3_breaking` (adds `channel` with no default) |

## Why the id goes in a header

- The payload stays plain Avro, so any tool that reads Avro can read it.
- Consumers can route, filter or skip a message by schema without decoding it.
- The same pattern works with JSON Schema or Protobuf, and with any registry that gives you a stable id.

## Status

Pinned: `apache-iggy` 0.9.0, `fastavro` 1.13.1, `httpx` 0.28.1, `apicurio/apicurio-registry:3.3.3`.
The Avro encode and decode path, including reading v1 data as v2, is tested locally.
The registry calls follow Apicurio's 3.3.3 OpenAPI spec. Not yet run end to end against live containers.

## Running Apache Iggy in production

LaserData builds and runs Apache Iggy as a managed service, or in your own AWS or GCP account: https://laserdata.com

Licensed under Apache 2.0.
