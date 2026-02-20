#!/usr/bin/env bash
set -euo pipefail

# Cleanup script for HBase Python driver test environment
# Removes containers, volumes, and networks related to the project

echo "🧹 HBase Python Driver - Cleanup Utility"
echo "========================================"
echo ""

# Find docker-compose command
COMPOSE="docker-compose"
if ! command -v docker-compose >/dev/null 2>&1; then
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
  else
    echo "docker-compose or 'docker compose' is required" >&2
    exit 1
  fi
fi

# Determine what to clean up
CLEAN_VOLUMES=0
CLEAN_IMAGES=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --volumes)
      CLEAN_VOLUMES=1
      shift
      ;;
    --images)
      CLEAN_IMAGES=1
      shift
      ;;
    --all)
      CLEAN_VOLUMES=1
      CLEAN_IMAGES=1
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [options]"
      echo ""
      echo "Options:"
      echo "  --volumes   Remove volumes (hbase-data)"
      echo "  --images    Remove Docker images"
      echo "  --all       Remove everything (containers, volumes, images, networks)"
      echo "  --help      Show this help message"
      echo ""
      echo "Default (no options): Stops and removes containers only"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

# Remove containers
echo "Stopping and removing containers..."
$COMPOSE down --remove-orphans || true

# Remove volumes if requested
if [ $CLEAN_VOLUMES -eq 1 ]; then
  echo "Removing volumes..."
  $COMPOSE down -v --remove-orphans || true
fi

# Remove images if requested
if [ $CLEAN_IMAGES -eq 1 ]; then
  echo "Removing Docker images..."
  docker rmi python-hbase-driver-hbase:latest 2>/dev/null || true
  docker rmi python-hbase-driver-dev:latest 2>/dev/null || true
  echo "Images removed"
fi

echo ""
echo "✅ Cleanup complete!"
echo ""
echo "Current status:"
docker ps -a --filter "name=hbase" --format "table {{.Names}}\t{{.Status}}" || echo "No hbase containers"
