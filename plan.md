# Plan

Checkpoint: 3-node HBase cluster setup
- Custom Docker images for ZooKeeper, HBase Master and RegionServers
- Shared /hbase-data Docker volume to support distributed mode
- New test orchestration script: scripts/run_tests_3node.sh
- Fixed ResultScanner behavior and connection fallbacks
- Removed single-node hostname hack

Status: Completed (2026-02-21)

Next steps:
- Add CI workflow (GitHub Actions) to run scripts/run_tests_3node.sh in CI and publish a test badge
- Expand Java-parity tests where gaps remain (replication, advanced admin operations)
- Consider integration with a lightweight HDFS or NFS-backed storage if testing needs persist across ephemeral containers

