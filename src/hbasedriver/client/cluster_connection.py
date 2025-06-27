from hbasedriver.zk_connection_registry import ZKConnectionRegistry

from abc import ABC, abstractmethod


class ClusterConnection(ABC):
    def __init__(self, conf: dict):
        self.conn_map = {}
        self.registry = ZKConnectionRegistry(conf)

    # Find the location of the region of <i>tableName</i> that <i>row</i> lives in.
    @abstractmethod
    def locate_region(self, table_name, row):
        pass
