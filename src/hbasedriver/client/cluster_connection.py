from hbasedriver.table_name import TableName
from hbasedriver.zk_connection_registry import ZKConnectionRegistry

from abc import ABC, abstractmethod


class ClusterConnection(ABC):
    def __init__(self, conf: dict):
        self.conn_map = {}
        self.registry = ZKConnectionRegistry(conf)

    # Find the location of the region of tableName that row lives in.
    @abstractmethod
    def locate_region(self, table_name, row) -> 'RegionLocations':
        # todo: finish this.
        if table_name == TableName.META_TABLE_NAME:
            return self.registry.get_meta_region_locations()
