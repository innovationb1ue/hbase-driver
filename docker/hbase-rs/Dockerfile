FROM eclipse-temurin:11-jre

ENV HBASE_VERSION=2.6.1 \
    HBASE_HOME=/opt/hbase \
    JAVA_HOME=/opt/java/openjdk \
    PATH=$PATH:/opt/hbase/bin

RUN apt-get update && apt-get install -y --no-install-recommends wget bash curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt && \
    cd /opt && \
    wget -q https://archive.apache.org/dist/hbase/${HBASE_VERSION}/hbase-${HBASE_VERSION}-bin.tar.gz && \
    tar xzf hbase-${HBASE_VERSION}-bin.tar.gz && \
    mv hbase-${HBASE_VERSION} hbase && \
    rm hbase-${HBASE_VERSION}-bin.tar.gz && \
    mkdir -p /hbase-data/data

# hbase-site.xml is injected at runtime (or copied here; we'll use a shared build arg)
COPY hbase-site.xml ${HBASE_HOME}/conf/hbase-site.xml
COPY start-rs.sh /start-rs.sh
RUN chmod +x /start-rs.sh

EXPOSE 16020 16030

VOLUME ["/hbase-data"]

WORKDIR ${HBASE_HOME}

CMD ["/start-rs.sh"]
