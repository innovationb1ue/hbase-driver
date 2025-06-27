from hbasedriver.client.cluster_connection import ClusterConnection
from hbasedriver.table_name import TableName
from hbasedriver.user import User


class ConnectionImplementation(ClusterConnection):
    def __init__(self, conf: dict, user: User, executors=None):
        super().__init__(conf)
        self.conf = conf
        self.user = user

    def locate_region(self, table_name: TableName, row):
        if table_name.ns == b'hbase' and table_name.tb == b'meta':

    def locate_meta(self):
        pass

    def locate_region_in_meta(self):
        pass