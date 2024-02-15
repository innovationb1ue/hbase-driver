from hbasedriver import zk
from hbasedriver.client.table import Table
from hbasedriver.master import MasterConnection


class Client:
    """
    Client class only contains Admin operations .

    Table and data manipulation actions are in class Table.
    Get Table instance by Client.get_table(ns, tb)
    """

    def __init__(self, zk_quorum: list):
        self.zk_quorum = zk_quorum
        self.master_host, self.master_port = zk.locate_master(zk_quorum)
        self.master_conn = MasterConnection().connect(self.master_host, self.master_port)

    def refresh_master(self):
        self.master_host, self.master_port = zk.locate_meta_region(self.zk_quorum)

    def create_table(self, ns, tb, columns, split_keys=None):
        self.master_conn.create_table(ns, tb, columns, split_keys)

    def get_table(self, ns, tb):
        return Table(self.zk_quorum, ns, tb)
