#!/usr/bin/env bash
set -euo pipefail

# Usage: ./build_upload.sh [test|prod] [install]
# Default: test (TestPyPI)
REPO=${1:-test}
DO_INSTALL=${2:-}

echo "Building distributions..."
rm -rf ./dist/*
python3 -m build

if [ "$DO_INSTALL" = "install" ]; then
    echo "Installing built wheel locally..."
    yes | pip uninstall -y hbase-driver || true
    pip install dist/hbase_driver-*-py3-none-any.whl
fi

if [ "$REPO" = "test" ]; then
    REPO_URL="https://test.pypi.org/legacy/"
elif [ "$REPO" = "prod" ] || [ "$REPO" = "pypi" ]; then
    REPO_URL="https://upload.pypi.org/legacy/"
else
    echo "Unknown repository: $REPO. Use 'test' or 'prod'."
    exit 1
fi

# Check for twine credentials; prefer TWINE_API_TOKEN, or TWINE_USERNAME/TWINE_PASSWORD
if [ -z "${TWINE_USERNAME-}" ] && [ -z "${TWINE_PASSWORD-}" ] && [ -z "${TWINE_API_TOKEN-}" ]; then
    echo "TWINE credentials not found in environment. To upload, set TWINE_API_TOKEN or TWINE_USERNAME and TWINE_PASSWORD."
    echo "Skipping upload."
    exit 0
fi

if [ -n "${TWINE_API_TOKEN-}" ]; then
    export TWINE_USERNAME="__token__"
    export TWINE_PASSWORD="${TWINE_API_TOKEN}"
fi

echo "Uploading to $REPO..."
python3 -m twine upload --repository-url "$REPO_URL" dist/*
