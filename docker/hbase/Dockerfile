FROM eclipse-temurin:11-jre

ENV HBASE_VERSION=2.6.1 \
    HBASE_HOME=/opt/hbase \
    JAVA_HOME=/opt/java/openjdk \
    PATH=$PATH:/opt/hbase/bin

RUN apt-get update && apt-get install -y --no-install-recommends wget bash curl && rm -rf /var/lib/apt/lists/*

# Download and install HBase
RUN mkdir -p /opt && \
    cd /opt && \
    wget https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz && \
    tar xzf hbase-${HBASE_VERSION}-bin.tar.gz && \
    mv hbase-${HBASE_VERSION} hbase && \
    rm hbase-${HBASE_VERSION}-bin.tar.gz && \
    mkdir -p /hbase-data/data

# Create hbase-site.xml for standalone mode with embedded ZooKeeper
RUN cat > ${HBASE_HOME}/conf/hbase-site.xml << 'EOF'
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>file:///hbase-data</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>false</value>
  </property>
  <property>
    <name>hbase.unsafe.stream.capability.enforce</name>
    <value>false</value>
  </property>
  <property>
    <name>hbase.zookeeper.useMultipleKeysPerServer</name>
    <value>false</value>
  </property>
  <property>
    <name>hbase.regionserver.ipc.address</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>hbase.master.ipc.address</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>localhost</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/hbase-data/zk</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.clientPortAddress</name>
    <value>0.0.0.0</value>
  </property>
</configuration>
EOF

# Expose HBase ports
# 16010: HBase Master web UI
# 16000: HBase Master IPC
# 16020: HBase RegionServer web UI
# 16030: HBase RegionServer IPC
EXPOSE 16010 16000 16020 16030 2181

VOLUME ["/hbase-data"]

WORKDIR ${HBASE_HOME}

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=10 \
  CMD curl -f http://localhost:16010/ || exit 1

CMD ["hbase", "master", "start"]
