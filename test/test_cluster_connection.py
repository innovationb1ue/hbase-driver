from hbasedriver.client.ConnectionImplementation import ConnectionImplementation
from hbasedriver.client.cluster_connection import ClusterConnection


def test_zk_registry():
    conf = {"hbase.zookeeper.quorum": ["127.0.0.1"]}
    conn = ConnectionImplementation(conf)
    res = conn.locate_meta()
    print(res)
