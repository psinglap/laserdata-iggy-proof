# feat(iggy): add Apache Iggy module

Adds `IggyContainer` for [Apache Iggy](https://iggy.apache.org), an Apache top-level project for
persistent message streaming. Iggy is a single binary with no ZooKeeper, KRaft or JVM, so a container
is ready in seconds, which makes it a good fit for producer and consumer integration tests.

## What's included

- `testcontainers.community.iggy.IggyContainer` (image `apache/iggy:0.9.0` by default)
  - `get_connection_string()` returns a URI the official SDKs accept, e.g. `iggy+tcp://iggy:iggy@localhost:49153`
  - `get_tcp_host_and_port()` and `get_http_url()`
  - Waits on Iggy's unauthenticated `/ping` HTTP route before returning from `start()`
- Tests: connection details, and a produce-then-consume round trip with the official `apache-iggy` SDK
- Docs page, usage example, `iggy` optional dependency and mkdocs entry

## Notes for reviewers

The server uses `io_uring`, which Docker's default seccomp profile blocks. The container therefore
starts with `seccomp:unconfined`, `SYS_NICE` and an unlimited `memlock`, the same settings as Iggy's
own `docker-compose.yml`. Happy to make these opt-out if you prefer.

The root user is created from `IGGY_ROOT_USERNAME` and `IGGY_ROOT_PASSWORD`, set from the
constructor's `username` and `password` (default `iggy`/`iggy`). Without them the server generates a
random root password on first start.

## Checklist

- [x] Follows the existing community module layout (`nats`, `kafka`)
- [x] Doctest in the class docstring
- [ ] `pytest tests/community/iggy` passing in CI <!-- confirm on the PR -->

Disclosure: I work at LaserData, which builds and runs Apache Iggy. Glad to maintain this module.
