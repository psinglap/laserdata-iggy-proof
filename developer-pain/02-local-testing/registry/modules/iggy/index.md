---
title: Apache Iggy
categories:
  - message-broker
docs:
  - id: python
    url: https://testcontainers-python.readthedocs.io/en/latest/modules/iggy/README.html
    maintainer: core
    example: |
      ```python
      with IggyContainer() as iggy:
          client = IggyClient.from_connection_string(iggy.get_connection_string())
          await client.connect()
      ```
    installation: |
      ```bash
      pip install testcontainers[iggy]
      ```
---
Apache Iggy is a persistent message streaming platform written in Rust, with partitioned streams, consumer groups and replay by offset or timestamp.
