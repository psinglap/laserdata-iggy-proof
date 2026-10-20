# Second pull request: list Apache Iggy on testcontainers.com/modules

Target repo: https://github.com/testcontainers/community-module-registry

Open this **after** the testcontainers-python module is merged, so the docs URL resolves.

1. Copy `modules/iggy/index.md` into the registry repo.
2. Add `modules/iggy/logo.svg`, using the official Apache Iggy logo from the Apache Iggy website repository (do not redraw it).
3. Add Java, Go and Node.js entries to `docs:` as those modules land.

The registry also supports an `officialPartner` block (Redpanda uses it). LaserData can apply for this once
it maintains modules in two or more languages.
