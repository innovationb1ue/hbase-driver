from hbasedriver.client.cluster_connection import ClusterConnection


def test_zk_registry():
    conf = {"hbase.zookeeper.quorum": ["127.0.0.1"]}
    conn = ClusterConnection(conf)
    conn.
    print(res)