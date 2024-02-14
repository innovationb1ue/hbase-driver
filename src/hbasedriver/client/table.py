from src.hbasedriver.master import MasterConnection
from src.hbasedriver.region_name import RegionName
from src.hbasedriver.regionserver import RsConnection
from src.hbasedriver.zk import locate_meta_region


class Table:
    """
    This class contains data operations within a table.
    """

    def __init__(self, zk_quorum, ns, tb):
        self.ns = ns
        self.tb = tb
        self.meta_rs_host, self.meta_rs_port = locate_meta_region(zk_quorum)
        # we might maintain connections to different regionserver.
        self.rs_connections = {}
        self.cache_regions = {}

    def put(self, rowkey, cf_to_qf_vals: dict):
        region = self.locate_target_region(rowkey)
        conn = self._get_and_cache_region_conn(rowkey)
        return conn.put(region.region_encoded, rowkey, cf_to_qf_vals)

    def get(self, rowkey, cf_to_qfs: dict):
        conn = self._get_and_cache_region_conn(rowkey)
        return conn.get(self.ns, self.tb, rowkey, cf_to_qfs)

    def delete(self, rowkey, cf_to_qfs):
        conn = self._get_and_cache_region_conn(rowkey)
        return conn.delete(self.ns, self.tb, rowkey, cf_to_qfs)

    def _get_and_cache_region_conn(self, rowkey: bytes):
        region = self.locate_target_region(rowkey)
        conn = self.rs_connections.get(region.region_encoded)
        if not conn:
            conn = RsConnection().connect(region.host, region.port)
            self.rs_connections[region.region_encoded] = conn
        return conn

    def locate_target_region(self, rowkey) -> RegionName:
        conn = RsConnection().connect(self.meta_rs_host, self.meta_rs_port)
        return conn.locate_region(self.ns, self.tb, rowkey)
