# Changelog

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
