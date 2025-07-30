#!/usr/bin/env bash
set -euo pipefail

HB_VERSION="2.6.3"
HB_DIR="hbase-$HB_VERSION"
HB_TARBALL="$HB_DIR-bin.tar.gz"
HB_SRC_TARBALL="$HB_DIR-src.tar.gz"


echo "1️⃣ Downloading HBase $HB_VERSION binary distribution..."
curl -LO "https://downloads.apache.org/hbase/$HB_VERSION/$HB_TARBALL"

echo "2️⃣ Extracting binary distribution..."
tar -xzf "$HB_TARBALL"

mv "$HB_DIR" hbase-bin

cp -rf hbase-site.xml hbase-bin/conf/

echo "3️⃣ Starting local HBase server..."
(cd hbase-bin && ./bin/start-hbase.sh)

echo "4️⃣ Downloading HBase $HB_VERSION source code..."
curl -LO "https://downloads.apache.org/hbase/$HB_VERSION/$HB_SRC_TARBALL"

echo "5️⃣ Extracting source code..."
tar -xzf "$HB_SRC_TARBALL"

pip install kazoo



echo "✅ Setup complete!"
echo "You now have:"
echo "  • Binary in $(pwd)/$HB_DIR"
echo "  • Source code in $(pwd)/$HB_DIR-src"
