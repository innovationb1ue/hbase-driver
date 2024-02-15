from hbasedriver.meta_server import MetaRsConnection
from hbasedriver.model.row import Row
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.get import Get
from hbasedriver.region import Region
from hbasedriver.regionserver import RsConnection
from hbasedriver.zk import locate_meta_region
from hbasedriver.operations.put import Put


class Table:
    """
    This class contains data operations within a table.
    """

    def __init__(self, zk_quorum, ns, tb):
        self.ns = ns
        self.tb = tb
        self.meta_rs_host, self.meta_rs_port = locate_meta_region(zk_quorum)
        # cache metadata for regions that we touched.
        self.regions = {}
        # we might maintain connections to different regionserver.
        self.rs_conns: dict[(bytes, int), RsConnection] = {}

    def put(self, put: Put):
        region: Region = self.locate_target_region(put.rowkey)
        conn = self.get_rs_connection(region)
        return conn.put(region.region_encoded, put)

    def get(self, get: Get):
        region: Region = self.locate_target_region(get.rowkey)
        conn = self.get_rs_connection(region)
        return conn.get(region.region_encoded, get)

    def delete(self, delete: Delete):
        """

        :param rowkey:
        :param cf_to_qfs:
        :return:
        """
        region: Region = self.locate_target_region(delete.rowkey)
        conn = self.get_rs_connection(region)
        return conn.delete(region, delete)

    def get_rs_connection(self, region: Region):
        conn = self.rs_conns.get((region.host, region.port))
        if not conn:
            conn: RsConnection = RsConnection().connect(region.host, region.port)
            self.rs_conns[(region.host, region.port)] = conn
        return conn

    def locate_target_region(self, rowkey) -> Region:
        # check cached regions first, return if we already touched that region.
        for region in self.regions.values():
            if region.key_in_region(rowkey):
                return region

        conn = MetaRsConnection().connect(self.meta_rs_host, self.meta_rs_port)
        region = conn.locate_region(self.ns, self.tb, rowkey)
        self.regions[region.region_info.region_id] = region
        return region
