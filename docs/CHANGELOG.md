# Changelog

## v1.1.0 - 2026-03-15

### New Features

#### BufferedMutator (Efficient Bulk Writes)
- Added `BufferedMutator` class for efficient bulk write operations with automatic buffering
- Configurable buffer size (default 2MB, minimum 64KB)
- Background flush thread with configurable flush interval
- Exception listener callback for handling write failures
- Context manager support with auto-flush on close
- Methods: `mutate()`, `mutate_all()`, `flush()`, `close()`, `get_current_buffer_size()`

#### Server-Side Atomic Operations
- **CheckAndPut**: Atomic conditional put operation
- **CheckAndDelete**: Atomic conditional delete operation
- **Increment**: Atomic counter increment with result return
- **Append**: Atomic value append with result return
- All operations execute atomically on the server side, eliminating race conditions

#### RowMutations (Atomic Multi-Mutation)
- Added `RowMutations` class for combining multiple mutations (Put, Delete, Increment, Append) into a single atomic operation
- All mutations on a row are applied together or none are applied
- Full compatibility with Java HBase client API

#### Exists Operations
- Added `table.exists(get)` for server-side row/column existence check
- Added `table.exists_all(gets)` for batch existence checks

### Admin Enhancements
- Added `compact_region()` for region compaction
- Added `flush_region()` for region memstore flush

### Bug Fixes
- Fixed protobuf field naming issues (`regionAction` vs `region_action`)
- Added `MultiResponse` to response type mapping for RowMutations support

### Tests
- Added 14 new tests for BufferedMutator (`test/test_buffered_mutator.py`)
- Added 13 new tests for atomic operations (`test/test_atomic_operations.py`)
- Added 12 new tests for RowMutations and exists (`test/test_row_mutations.py`)
- Total test suite: 238 tests with comprehensive coverage

### Documentation
- Updated README.md with new feature examples
- Updated api_reference.md with comprehensive API documentation
- Updated advanced_usage.md with detailed usage patterns
- Added BufferedMutator, atomic operations, and RowMutations documentation


## v1.0.2 - 2026-02-21

- Feature: Added a reproducible custom 3-node HBase cluster for integration testing (ZooKeeper, Master, 3x RegionServers) built from our Dockerfiles under docker/.
- Feature: Added scripts/run_tests_3node.sh to build, start, validate readiness (ZK, meta, RS registrations, master DDL) and run the full pytest suite against the 3-node cluster.
- Fix: Use a single shared Docker volume (hbase-shared-data) for /hbase-data so Master and RegionServers share metadata and table descriptors (resolves Master initialization / Missing table descriptor issues).
- Fix: ResultScanner: avoid infinite scan loops across regions by detecting same-region locate_region results and updating scan.start_row when advancing to the next region.
- Fix: Removed single-node hostname remapping workaround (region.py) — RegionServer hostnames are now injected as service hostnames (hbase-rs1/2/3) at container startup.
- Improvement: Connection IPv4 fallback for EADDRNOTAVAIL during socket resolution.
- Docs: Added run_tests_3node.sh, updated README, DEV_ENV.md, TEST_GUIDE.md and RELEASE.md to document the multi-node cluster workflow and testing status.


## v1.0.1 - previous

- Fix: Truncate flakiness by validating and refreshing cached region metadata (Table.locate_target_region).
- Feature: Implemented additional admin namespace APIs and related tests.
- Packaging: Added protobuf runtime as a dependency to ensure generated protobuf modules work.


Notes

- Full integration test status: 77 / 77 tests pass locally against the custom 3-node cluster (2026-02-21).
- See `scripts/run_tests_3node.sh` for automated cluster build + test orchestration and `scripts/run_tests_docker.sh` for the single-node dev workflow.
