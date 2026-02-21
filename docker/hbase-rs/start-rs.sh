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

# Wait for HBase master
echo "Waiting for HBase Master at hbase-master:16000..."
for i in $(seq 1 60); do
  if nc -z hbase-master 16000 2>/dev/null; then
    echo "HBase Master is ready"
    break
  fi
  sleep 2
  if [ "$i" -eq 60 ]; then
    echo "Timed out waiting for HBase Master"
    exit 1
  fi
done

# Inject hostname into hbase-site.xml if provided
if [ -n "${HBASE_RS_HOSTNAME:-}" ]; then
  echo "Setting hbase.regionserver.hostname to ${HBASE_RS_HOSTNAME}"
  sed -i "s|</configuration>|  <property><name>hbase.regionserver.hostname</name><value>${HBASE_RS_HOSTNAME}</value></property>\n</configuration>|" "${HBASE_HOME}/conf/hbase-site.xml"
fi

exec hbase regionserver start
