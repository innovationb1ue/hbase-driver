# Test Guide

Quick Start (3-node cluster)

Prerequisites
- Docker and Docker Compose

Run everything (build + start + tests):

```bash
./scripts/run_tests_3node.sh
```

Run against an already-running cluster:

```bash
./scripts/run_tests_3node.sh --no-start
```

Test summary (local)

- Full integration suite: **77 / 77 tests passing** (verified on 2026-02-21 using the custom 3-node HBase cluster)

If tests fail on first run, inspect container logs and rerun with the --no-start option after resolving issues.

