Dev environment: docker-compose-based HBase and Python dev container

To start the custom 3-node cluster and run tests:

  ./scripts/run_tests_3node.sh

This script will:
- Build custom ZooKeeper, HBase Master and RegionServer images
- Create a shared Docker volume for /hbase-data used by Master and RSes
- Start ZK, Master, 3x RS, and the dev container
- Wait for ZK, meta region, RS registration and master DDL readiness
- Run pytest inside the dev container

For quick iterations where the cluster is already running use:

  ./scripts/run_tests_3node.sh --no-start

For the single-node dev environment (legacy) use:

  ./scripts/run_tests_docker.sh

