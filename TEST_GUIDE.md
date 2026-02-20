# HBase Python Driver - Test Guide

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Bash shell

### Running Tests

#### Run All Tests
```bash
./scripts/run_tests_docker.sh
```

#### Run a Single Test File
```bash
./scripts/run_tests_docker.sh test/test_zk.py
```

#### Run a Single Test Case
```bash
./scripts/run_tests_docker.sh test/test_zk.py::test_locate_meta
```

#### Run Tests with Verbose Output
```bash
./scripts/run_tests_docker.sh -v
```

#### Run Tests Without Rebuilding Docker Images
```bash
./scripts/run_tests_docker.sh --no-build
```

#### Run Tests Using Already-Running Containers
```bash
./scripts/run_tests_docker.sh --no-start
```

#### Get Help
```bash
./scripts/run_tests_docker.sh --help
```

## Docker Environment

The test setup uses Docker Compose to spin up:

1. **HBase 2.6.1** (`hbase` service)
   - Standalone mode with embedded ZooKeeper
   - Ports: 16010 (Web UI), 16000/16030 (IPC), 2181 (ZK)
   - Data persisted in Docker volume `hbase-data`

2. **Dev Container** (`hbase_dev` service)
   - Python 3.11 with all driver dependencies
   - Mounts project directory for live code changes
   - Runs pytest tests against HBase instance

## Test Structure

```
test/
├── test_admin_namespace.py     # Admin namespace operations
├── test_client.py              # Client put/get/delete operations
├── test_cluster_connection.py  # Cluster connectivity tests
├── test_examples.py            # Examples.py functionality
├── test_master.py              # Master operations (create/delete/describe tables)
├── test_rs.py                  # Region server operations
├── test_scan.py                # Scan operations
├── test_truncate.py            # Truncate table operations
├── test_zk.py                  # ZooKeeper metadata operations
└── conftest.py                 # Pytest configuration and fixtures
```

## Test Results

**Current Status: 25/25 tests passing (100% pass rate)**

All integration tests validate:
- Client connectivity and operations
- Admin operations (create/delete/disable/enable tables)
- Namespace management
- Table scanning and pagination
- ZooKeeper coordination
- Region server operations

## Container Management

### View Container Status
```bash
docker-compose ps
```

### View HBase Logs
```bash
docker-compose logs hbase
```

### View Dev Container Logs
```bash
docker-compose logs hbase_dev
```

### Stop Containers
```bash
docker-compose down
```

### Clean Everything (including data)
```bash
docker-compose down -v
```

## Environment Variables

### HBASE_ZK
Default ZooKeeper quorum address inside containers: `hbase:2181`

To override (rarely needed):
```bash
HBASE_ZK="my-zk:2181" ./scripts/run_tests_docker.sh
```

## Troubleshooting

### Tests Fail on First Run
The HBase container may take 30-60 seconds to fully initialize. The script includes waits for HTTP and ZK readiness. If tests still fail:

```bash
# Check HBase logs
docker-compose logs hbase

# Verify ZK is accessible
docker exec hbase_dev python -c "from kazoo.client import KazooClient; z=KazooClient(hosts='hbase:2181'); z.start(); print('ZK OK'); z.stop()"
```

### Container Won't Start
```bash
# Remove old containers and volumes
docker-compose down -v

# Restart fresh
./scripts/run_tests_docker.sh
```

### Port Already in Use
If ports 16010, 16000, 16020, 16030, or 2181 are already in use, modify `docker-compose.yml`:
```yaml
hbase:
  ports:
    - "8010:16010"    # Change 16010 to 8010 on host
    - "8000:16000"    # etc...
```

## Development Workflow

### Making Code Changes
1. Edit driver code in `src/hbasedriver/`
2. Run specific test to verify fix:
   ```bash
   ./scripts/run_tests_docker.sh --no-start test/test_file.py::test_name -v
   ```
3. Run full suite before commit:
   ```bash
   ./scripts/run_tests_docker.sh --no-start --no-build
   ```

### Interactive Testing
```bash
# Shell into dev container
docker exec -it hbase_dev bash

# Inside container, run pytest directly
pytest test/test_zk.py -v
```

### Keeping HBase Running Between Test Runs
```bash
# Start once
./scripts/run_tests_docker.sh

# Use --no-start for subsequent runs (much faster)
./scripts/run_tests_docker.sh --no-start test/test_zk.py
./scripts/run_tests_docker.sh --no-start test/test_client.py
```

## Advanced Usage

### Run Tests with Different Python Markers
```bash
# Run tests matching a pattern
./scripts/run_tests_docker.sh -k "admin"

# Run tests excluding a pattern
./scripts/run_tests_docker.sh -k "not scan"
```

### Run with Code Coverage
```bash
./scripts/run_tests_docker.sh --cov=src/hbasedriver
```

### Run with Parallel Execution
```bash
./scripts/run_tests_docker.sh -n 4
```

## CI/CD Integration

The test script is designed to be CI/CD friendly:
- Returns exit code 0 on success, 1 on failure
- Clean output suitable for log aggregation
- Docker cleanup via `docker-compose down -v` if needed

Example GitHub Actions workflow:
```yaml
- name: Run HBase Driver Tests
  run: ./scripts/run_tests_docker.sh
```

## Performance Notes

- First run: ~3-5 minutes (builds images + HBase initialization)
- Subsequent runs: ~50 seconds (with --no-build --no-start)
- Full test suite execution: ~45 seconds

## Tips

- Use `--no-build --no-start` flags to skip overhead when making rapid iterations
- Run targeted tests first to validate logic before full suite
- Keep containers running between test sessions for faster feedback
- Check HBase web UI at http://localhost:16010 while tests run
