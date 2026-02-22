from typing import TYPE_CHECKING

from hbasedriver.client.result_scanner import ResultScanner
from hbasedriver.client.cluster_connection import ClusterConnection
from hbasedriver.meta_server import MetaRsConnection
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.get import Get
from hbasedriver.operations.scan import Scan
from hbasedriver.region import Region
from hbasedriver.regionserver import RsConnection
from hbasedriver.table_name import TableName
from hbasedriver.zk import locate_meta_region
from hbasedriver.operations.put import Put

if TYPE_CHECKING:
    from hbasedriver.model import Row


class Table:
    """
    This class contains data operations within a table.
    """

    def __init__(self, conf: dict, ns: bytes | str, tb: bytes | str) -> None:
        # Ensure ns and tb are bytes
        if isinstance(ns, str):
            ns = ns.encode('utf-8')
        if isinstance(tb, str):
            tb = tb.encode('utf-8')

        self.ns: bytes = ns
        self.tb: bytes = tb
        self.conf: dict = conf
        self.meta_rs_host: str
        self.meta_rs_port: int
        self.meta_rs_host, self.meta_rs_port = locate_meta_region(conf.get("hbase.zookeeper.quorum").split(","))
        # cache metadata for regions that we touched.
        self.regions: dict[int, Region] = {}
        # we might maintain connections to different regionserver.
        self.rs_conns: dict[tuple[bytes, int], RsConnection] = {}
        self.cluster_conn: ClusterConnection | None = None

    def put(self, put: Put) -> bool:
        region: Region = self.locate_target_region(put.rowkey)
        conn = self.get_rs_connection(region)
        return conn.put(region.region_encoded, put)

    def get(self, get: Get) -> 'Row | None':
        region: Region = self.locate_target_region(get.rowkey)
        conn = self.get_rs_connection(region)
        return conn.get(region.region_encoded, get)

    def delete(self, delete: Delete) -> bool:
        """
        :param delete:
        :param rowkey:
        :param cf_to_qfs:
        :return:
        """
        region: Region = self.locate_target_region(delete.rowkey)
        conn = self.get_rs_connection(region)
        return conn.delete(region, delete)

    def scan(self, scan: Scan) -> ResultScanner:
        # Use cluster-level scanner which will locate regions and iterate across them
        return self.get_scanner(scan)

    def get_scanner(self, scan: Scan) -> ResultScanner:
        # Ensure a cluster connection is available so scanners can locate regions spanning the cluster
        if self.cluster_conn is None:
            self.cluster_conn = ClusterConnection(self.conf)
        return ResultScanner(scan, TableName.value_of(self.ns, self.tb), self.cluster_conn)

    def scan_page(self, scan: Scan, page_size: int) -> tuple[list['Row'], bytes | None]:
        """Stateless pagination helper: open a scanner, fetch up to page_size rows, close and return (rows, resume_key)

        Resume key is the last returned row's key (client should use start_row=resume_key and include_start_row=False to continue).
        """
        scanner = self.get_scanner(scan)
        try:
            rows = scanner.next_batch(page_size)
            resume = None
            if rows:
                resume = rows[-1].rowkey
            return rows, resume
        finally:
            try:
                scanner.close()
            except Exception:
                pass

    def get_rs_connection(self, region: Region) -> RsConnection:
        conn = self.rs_conns.get((region.host, region.port))
        if not conn:
            conn: RsConnection = RsConnection().connect(region.host, region.port)
            self.rs_conns[(region.host, region.port)] = conn
        return conn

    def locate_target_region(self, rowkey: bytes) -> Region:
        # check cached regions first, return if we already touched that region.
        # Validate cached entries against meta to avoid using stale encoded region names
        # that can occur after delete+create (e.g., truncate fallback).
        for region in list(self.regions.values()):
            if region.key_in_region(rowkey):
                try:
                    meta_conn = MetaRsConnection().connect(self.meta_rs_host, self.meta_rs_port)
                    fresh = meta_conn.locate_region(self.ns, self.tb, rowkey)
                    # If the encoded name matches, cached entry is still valid
                    if fresh.get_region_name() == region.get_region_name():
                        return region
                    # Otherwise replace stale cache with fresh region info
                    self.regions[fresh.region_info.region_id] = fresh
                    try:
                        del self.regions[region.region_info.region_id]
                    except Exception:
                        pass
                    return fresh
                except Exception:
                    # If meta lookup fails, fall back to cached region to avoid breaking callers.
                    return region

        conn = MetaRsConnection().connect(self.meta_rs_host, self.meta_rs_port)
        region = conn.locate_region(self.ns, self.tb, rowkey)
        self.regions[region.region_info.region_id] = region
        return region
