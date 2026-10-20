# Apache Iggy

Since testcontainers-python <span class="tc-version">:material-tag: vNEXT</span>

## Introduction

The Testcontainers module for [Apache Iggy](https://iggy.apache.org), a persistent message streaming
server written in Rust. One container starts in seconds, with no ZooKeeper, KRaft or JVM, which keeps
integration tests for producers and consumers fast.

## Adding this module to your project dependencies

```bash
pip install testcontainers[iggy]
```

The `iggy` extra installs `apache-iggy`, the official Python SDK.

## Usage example

<!--codeinclude-->

[Producing and consuming with Apache Iggy](iggy_example.py)

<!--/codeinclude-->
