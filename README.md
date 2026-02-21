# hbase-driver

[![Tests](https://github.com/innovationb1ue/hbase-driver/actions/workflows/ci.yml/badge.svg)](https://github.com/innovationb1ue/hbase-driver/actions)
[![License](https://img.shields.io/badge/license-Apache%202-blue.svg)](LICENSE)

Pure-Python native HBase client (no Thrift). This project implements core HBase regionserver and master RPCs so Python programs can perform table and metadata operations against an HBase cluster.

Status

- Integration test status (local): **77 / 77 tests passing** (2026-02-21) using the custom 3-node Docker cluster.

Quickstart (3-node Docker dev environment)

Prerequisites: Docker and docker-compose installed.

1. Build, start the custom 3-node cluster and run the full test suite:

```bash
./scripts/run_tests_3node.sh
```

2. To run tests against an already-running cluster (fast):

```bash
./scripts/run_tests_3node.sh --no-start
```

3. Run a single test file or case:

```bash
./scripts/run_tests_3node.sh test/test_scan.py
./scripts/run_tests_3node.sh test/test_scan.py::test_scan
```

Legacy single-node dev workflow (still available):

```bash
./scripts/run_tests_docker.sh
```

See TEST_GUIDE.md and DEV_ENV.md for full documentation and troubleshooting steps.

