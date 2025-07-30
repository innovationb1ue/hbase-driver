import time

from hbasedriver import zk
from hbasedriver.client.admin import Admin
from hbasedriver.client.cluster_connection import ClusterConnection
from hbasedriver.client.table import Table
from hbasedriver.common.table_name import TableName
from hbasedriver.master import MasterConnection
from hbasedriver.meta_server import MetaRsConnection
from hbasedriver.operations import Get
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest, Column, ScanResponse
from hbasedriver.protobuf_py.HBase_pb2 import TableState
from hbasedriver.region import Region
from hbasedriver.zk import locate_meta_region


class Client:
    """
    Client acts like HBase Java Connection.
    Provides access to Admin, Table, and region metadata.
    """

    def __init__(self, conf: dict):
        self.zk_quorum = conf.get("hbase_zookeeper.quorum")
        self.master_host, self.master_port = zk.locate_master(self.zk_quorum)
        self.meta_host, self.meta_port = zk.locate_meta_region(self.zk_quorum)

        self.cluster_connection = ClusterConnection(conf)

        self.master_conn = MasterConnection().connect(self.master_host, self.master_port)
        self.meta_conn = MetaRsConnection().connect(self.meta_host, self.meta_port)

    def get_admin(self) -> Admin:
        return Admin(self)

    def get_table(self, ns: bytes | None, tb: bytes) -> Table:
        return Table(self.zk_quorum, ns or b"default", tb)

    def check_regions_online(self, ns: bytes, tb: bytes, split_keys: list[bytes]):
        time.sleep(1)
        attempts = 0
        expected = len(split_keys) or 1  # At least 1 region

        while attempts < 10:
            region_states = self.get_region_states(ns, tb)
            online = sum(1 for state in region_states.values() if state == "OPEN")
            if online >= expected:
                return
            time.sleep(3)
            attempts += 1

        raise RuntimeError("Timeout: Not all regions came online after table creation.")

    def get_table_state(self, ns: bytes, tb: bytes) -> TableState | None:
        """
        Returns the logical table state ('ENABLED', 'DISABLED', etc.) from hbase:meta using a direct Get.
        This does not reflect region-level state but the table metadata state.
        """
        # Construct the logical table rowkey
        if not ns or ns == b"default":
            rowkey = tb
        else:
            rowkey = ns + b":" + tb

        get = Get(rowkey)
        get.add_column(b"table", b"state")

        result = self.meta_conn.get(b"hbase:meta,,1", get)

        # table does not exist.
        if result is None:
            return None

        val = result.get(b"table", b"state")
        state = TableState()
        state.ParseFromString(val)
        return state

    def get_region_states(self, ns: bytes, tb: bytes) -> dict[str, str]:
        """
        Returns a map from region encoded name to region state ('OPEN', 'CLOSED') for the given table only.
        Filters out unrelated table and non-region rows.
        """
        rq = ScanRequest()

        # Compute scan prefix
        if not ns or ns == b"default":
            prefix = tb
        else:
            prefix = ns + b":" + tb  # matches full table name in HBase

        # Compute stop row to avoid scanning other tables
        stop_row = bytearray(prefix)
        for i in reversed(range(len(stop_row))):
            if stop_row[i] != 0xFF:
                stop_row[i] += 1
                stop_row = stop_row[:i + 1]
                break
        else:
            stop_row.append(0x00)

        rq.scan.start_row = bytes(prefix)
        rq.scan.include_start_row = True;
        rq.scan.stop_row = bytes(stop_row)
        rq.scan.column.append(Column(family=b"info"))
        rq.number_of_rows = 1000
        rq.region.type = 1
        rq.region.value = b"hbase:meta,,1"

        scan_resp: ScanResponse = self.meta_conn.send_request(rq, "Scan")

        region_states = {}
        for result in scan_resp.results:
            # Skip table-level state entries (e.g., row key == table name with no region suffix)
            rowkey = result.cell[0].row
            if rowkey.startswith(prefix) and b',' in rowkey[len(prefix):]:
                # this is a region-level row
                region = Region.from_cells(result.cell)
                encoded_name = region.get_region_name()
                state = None
                for cell in result.cell:
                    if cell.family == b"info" and cell.qualifier == b"state":
                        state = cell.value.decode("utf-8")
                        break
                if state:
                    region_states[encoded_name] = state

        return region_states

    def get_region_in_state_count(self, ns: bytes, tb: bytes, target_state: str, timeout=10):
        start = time.time()
        while True:
            states = self.get_region_states(ns, tb)
            count = sum(1 for s in states.values() if s == target_state)
            if count == len(states):
                return count
            if time.time() - start > timeout:
                raise TimeoutError(f"Timeout waiting for all regions in state: {target_state}")
            time.sleep(1)

    def describe_table(self, ns: bytes, tb: bytes):
        return self.master_conn.describe_table(ns, tb)

    def locate_region(self, table_name: TableName, row: bytes):
        if table_name == TableName.META_TABLE_NAME:
            return locate_meta_region(self.zk_quorum)
        return self.meta_conn.locate_region(table_name.ns, table_name.tb, row)

    def __rebuild_connection(self):
        self.master_host, self.master_port = zk.locate_master(self.zk_quorum)
        self.meta_host, self.meta_port = zk.locate_meta_region(self.zk_quorum)
        self.master_conn = MasterConnection().connect(self.master_host, self.master_port)
        self.meta_conn = MetaRsConnection().connect(self.meta_host, self.meta_port)
