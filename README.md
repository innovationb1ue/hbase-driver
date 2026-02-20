# hbase-driver

Pure-Python native HBase client (no Thrift). This project implements core HBase regionserver and master RPCs so Python programs can perform table and metadata operations against an HBase cluster.

Key goals:

- Provide a thin, idiomatic Python driver that mirrors the Java HBase client where practical.
- Support both regionserver data operations (Put/Get/Scan/Delete) and master/admin operations (create/delete/describe table, namespaces, truncate, etc.).
- Provide a reproducible Docker-based development environment that runs HBase + ZooKeeper and a separate dev container for running tests against the local HBase instance.

Status (high level)

- Latest release: 1.0.1 — includes admin namespace APIs, truncate robustness, protobuf runtime requirement, Docker dev environment, and packaging fixes.
- Implemented (non-exhaustive):
  - Table operations: create, disable, delete, describe
  - Data operations: Put, Get, Scan, Delete
  - Admin operations: create_namespace, delete_namespace, list_namespaces, truncate_table (with robust fallback)
  - Region location caching with validation against hbase:meta (reduces NotServingRegionException after truncate/delete+recreate)

Quickstart (Docker dev environment)

Prerequisites: Docker and docker-compose installed.

1. Start the development stack (HBase with embedded ZooKeeper, dev container):

   ```bash
   ./scripts/run_tests_docker.sh
   ```

   This starts HBase and runs the integration tests. The script automatically:
   - Builds Docker images
   - Starts HBase (2.6.1 with embedded ZK)
   - Waits for HBase to be ready
   - Runs pytest

2. For subsequent test runs (faster, skips build and HBase restart):

   ```bash
   ./scripts/run_tests_docker.sh --no-start --no-build
   ```

3. Run a specific test:

   ```bash
   ./scripts/run_tests_docker.sh test/test_zk.py::test_locate_meta
   ```

See [TEST_GUIDE.md](TEST_GUIDE.md) for complete testing documentation and examples.

Local development without Docker

- Install the package and runtime requirements locally for development:

  ```bash
  python -m pip install -e .
  python -m pip install protobuf
  pytest -q
  ```

  Note: many integration tests expect a running HBase; running tests in the Docker dev container is the simplest reproducible path.

Usage example

```python
from hbasedriver.client import Client
from hbasedriver.operations import Put, Get, Scan

# Connect using ZooKeeper quorum addresses
client = Client(["127.0.0.1"])  # example: local HBase dev stack

tbl = client.get_table("", "mytable")
# Put a value
tbl.put(Put(b"row1").add_column(b"cf", b"q", b"value"))
# Get a value
res = tbl.get(Get(b"row1").add_column(b"cf", b"q"))
print(res)
# Scan
for r in tbl.scan(Scan(b"row0")):
    print(r)
```

Packaging & publishing

- A helper script build_upload.sh is included to build sdist/wheel and upload to TestPyPI or PyPI.
  - For TestPyPI (test): `./build_upload.sh test`
  - For production PyPI (prod): `./build_upload.sh prod`
- Authentication options:
  - Use a PyPI token (recommended) via environment variables in CI: set TWINE_API_TOKEN for production, or create a token on TestPyPI for test uploads.
  - Example (token upload):
    - For TestPyPI: `TWINE_USERNAME=__token__ TWINE_PASSWORD=<test-token> twine upload --repository test dist/*`
    - For PyPI: `TWINE_USERNAME=__token__ TWINE_PASSWORD=<prod-token> twine upload --repository pypi dist/*`
  - Alternatively, configure credentials in `~/.pypirc` and run `./build_upload.sh prod`.
- Security: never commit API tokens to the repository; use CI secrets for automated uploads.

Notes and gotchas

- The driver requires the `protobuf` runtime to import the generated protobuf modules — protobuf is declared in pyproject.toml and should be installed in the environment.
- HBase cluster quirks: some admin procedures (truncate, namespace ops) can be asynchronous; tests include waits/retries and the driver includes fallbacks (delete+recreate table) to achieve deterministic behavior in small single-node dev clusters.
- When uploading to TestPyPI, ensure the API token was created on test.pypi.org (tokens are scoped to a registry).

Files of interest

- scripts/run_tests_docker.sh — main test orchestration script (handles build, startup, wait, and pytest execution)
- docker-compose.yml — defines HBase (with embedded ZK) and dev service
- docker/hbase/Dockerfile — custom HBase 2.6.1 image with embedded ZooKeeper
- docker/Dockerfile — dev container image with Python, pytest, and project dependencies
- docker/entrypoint.sh — entry point for dev container
- build_upload.sh — build and upload helper for TestPyPI / PyPI
- pyproject.toml / setup.py — packaging metadata and dependencies (including protobuf)
- TEST_GUIDE.md — comprehensive testing documentation
- src/hbasedriver/ — driver implementation (client, admin wrappers, protobuf bindings, utilities).
- test/ — integration tests that run against the containerized HBase instance.

Contributing

- Use the Docker dev environment to run and iterate on tests quickly:
  - `docker-compose up --build -d` then `docker-compose exec dev bash` then `pytest -q`.
- Open PRs for additional master RPCs or parity with the Java HBase client.

License

This project is released under the terms of the LICENSE file in this repository.


## Runnable examples (tested)

A small, test-friendly examples module (examples.py) is included that exercises the public
Client/Admin/Table APIs without requiring a running HBase instance. The helpers accept a
`client` object (the real `hbasedriver.client.Client` when used against a live cluster, or a
lightweight fake client for unit tests).

Files:

- examples.py — provides create_table_example, data_ops_example, admin_namespace_example,
  truncate_table_example, describe_table_example. These helpers avoid importing generated
  protobuf types so they are safe to import and run in CI.
- test/test_examples.py — unit tests that validate the examples using fake clients (no HBase).

Run the example tests locally:

```bash
pytest -q test/test_examples.py
```

For real usage, call the examples with a configured client, for example:

```python
from hbasedriver.client.client import Client
import examples

conf = {"hbase.zookeeper.quorum": "127.0.0.1"}
client = Client(conf)

# Create a table (real code should pass proper ColumnFamilySchema objects)
examples.create_table_example(client, b"default", b"mytable")

# Basic data operations
res, rows = examples.data_ops_example(client, b"default", b"mytable")
print(res)
```

