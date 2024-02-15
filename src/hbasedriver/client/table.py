from src.hbasedriver.meta_server import MetaRsConnection
from src.hbasedriver.region import Region
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
        # cache metadata for regions that we touched.
        self.regions = {}
        # we might maintain connections to different regionserver.
        self.rs_conns: dict[(bytes, int), RsConnection] = {}

    def put(self, rowkey: bytes, cf_to_qf_vals: dict):
        if type(rowkey) != bytes:
            raise ValueError("must provide bytes for rowkey ")
        region: Region = self.locate_target_region(rowkey)
        conn = self.get_rs_connection(region)
        return conn.put(region.region_encoded, rowkey, cf_to_qf_vals)

    def get(self, rowkey: bytes, cf_to_qfs: dict):
        if type(rowkey) != bytes:
            raise ValueError("must provide bytes for rowkey ")
        region: Region = self.locate_target_region(rowkey)
        conn = self.get_rs_connection(region)
        return conn.get(region, rowkey, cf_to_qfs)

    def delete(self, rowkey: bytes, cf_to_qfs):
        if type(rowkey) != bytes:
            raise ValueError("must provide bytes for rowkey ")
        region: Region = self.locate_target_region(rowkey)
        conn = self.get_rs_connection(region)
        return conn.delete(region, rowkey, cf_to_qfs)

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
