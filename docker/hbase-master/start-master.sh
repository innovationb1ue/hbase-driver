#!/usr/bin/env bash
set -euo pipefail

# Wait for ZooKeeper
echo "Waiting for ZooKeeper at hbase-zk:2181..."
for i in $(seq 1 60); do
  if nc -z hbase-zk 2181 2>/dev/null; then
    echo "ZooKeeper is ready"
    break
  fi
  sleep 2
  if [ "$i" -eq 60 ]; then
    echo "Timed out waiting for ZooKeeper"
    exit 1
  fi
done

exec hbase master start
