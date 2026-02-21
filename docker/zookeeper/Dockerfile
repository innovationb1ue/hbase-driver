FROM eclipse-temurin:11-jre

ENV ZK_VERSION=3.8.4 \
    ZK_HOME=/opt/zookeeper \
    PATH=$PATH:/opt/zookeeper/bin

RUN apt-get update && apt-get install -y --no-install-recommends wget bash curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt && \
    cd /opt && \
    wget -q https://archive.apache.org/dist/zookeeper/zookeeper-${ZK_VERSION}/apache-zookeeper-${ZK_VERSION}-bin.tar.gz && \
    tar xzf apache-zookeeper-${ZK_VERSION}-bin.tar.gz && \
    mv apache-zookeeper-${ZK_VERSION}-bin zookeeper && \
    rm apache-zookeeper-${ZK_VERSION}-bin.tar.gz && \
    mkdir -p /data/zk

COPY zoo.cfg /opt/zookeeper/conf/zoo.cfg

EXPOSE 2181 2888 3888

HEALTHCHECK --interval=5s --timeout=5s --start-period=20s --retries=10 \
    CMD echo ruok | nc localhost 2181 | grep imok || exit 1

CMD ["zkServer.sh", "start-foreground"]
