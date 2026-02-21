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

# Copy hbase-site.xml and regionservers list
COPY hbase-site.xml ${HBASE_HOME}/conf/hbase-site.xml
COPY regionservers  ${HBASE_HOME}/conf/regionservers
COPY start-master.sh /start-master.sh
RUN chmod +x /start-master.sh

EXPOSE 16000 16010 16020 16030

VOLUME ["/hbase-data"]

WORKDIR ${HBASE_HOME}

CMD ["/start-master.sh"]
