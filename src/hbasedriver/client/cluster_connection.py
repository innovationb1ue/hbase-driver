from hbasedriver.zk_connection_registry import ZKConnectionRegistry


class ClusterConnection:
    def __init__(self, conf: dict):
        self.conn_map = {}
        self.registry = ZKConnectionRegistry(conf)
