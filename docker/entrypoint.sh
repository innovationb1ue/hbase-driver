#!/usr/bin/env bash
set -euo pipefail

# Wait for HBase master (port 16010) if service exists
if getent hosts hbase >/dev/null 2>&1; then
  echo "Waiting for HBase master (hbase:16010)..."
  for i in $(seq 1 120); do
    if nc -z hbase 16010; then
      echo "HBase master is up"
      break
    fi
    sleep 1
    if [ "$i" -eq 120 ]; then
      echo "Timed out waiting for HBase"
      exit 1
    fi
  done
fi

# Install local package in editable mode
if [ -f setup.py ] || [ -f pyproject.toml ]; then
  pip install -e .
fi

exec "$@"
