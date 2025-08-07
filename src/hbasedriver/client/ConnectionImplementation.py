from hbasedriver.client.cluster_connection import ClusterConnection
from hbasedriver.region_locations import RegionLocations
from hbasedriver.table_name import TableName
from hbasedriver.user import User
from hbasedriver.zk_connection_registry import ZKConnectionRegistry


class ConnectionImplementation(ClusterConnection):
    def __init__(self, conf: dict, user: User = None, executors=None):
        super().__init__(conf)
        self.conf = conf
        self.user = user
        self.registry = ZKConnectionRegistry(conf)

    def locate_region(self, table_name: TableName, row, use_cache=True) -> RegionLocations:
        if table_name.ns == b'hbase' and table_name.tb == b'meta':
            return self.locate_meta()
        else:
            return self.locate_region_in_meta()

    def locate_meta(self) -> RegionLocations:
        return self.registry.get_meta_region_locations()
