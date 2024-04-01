import time

from hbasedriver import zk
from hbasedriver.client.table import Table
from hbasedriver.master import MasterConnection
from hbasedriver.meta_server import MetaRsConnection
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest, Column, ScanResponse
from hbasedriver.region import Region


class Client:
    """
    Client class only contains Admin operations .

    Table and data manipulation actions are in class Table.
    Get Table instance by Client.get_table(ns, tb)

    """

    def __init__(self, zk_quorum: list):
        self.zk_quorum = zk_quorum
        self.master_host, self.master_port = zk.locate_master(zk_quorum)
        self.meta_host, self.meta_port = zk.locate_meta_region(zk_quorum)

        self.master_conn = MasterConnection().connect(self.master_host, self.master_port)
        self.meta_conn = MetaRsConnection().connect(self.meta_host, self.meta_port)

    def refresh_master(self):
        self.master_host, self.master_port = zk.locate_meta_region(self.zk_quorum)

    def create_table(self, ns: bytes, tb: bytes, columns, split_keys=None):
        self.master_conn.create_table(ns, tb, columns, split_keys)
        # check regions online
        self.check_regions_online(ns, tb, split_keys)

    def check_regions_online(self, ns: bytes, tb: bytes, split_keys: list):
        # Construct a ScanRequest to check if regions are online for each split key
        online_regions = 0
        fail_count = 0
        # we get it here waiting no time will result to empty result.
        time.sleep(1)

        while online_regions != len(split_keys):
            # get all region states and check the keys.
            # todo: do we need to check the keys match the provided splitkey?
            rs = self.get_region_states(ns, tb)

            # check region status
            online_regions = 0
            for region_state in rs.values():
                if region_state == 'OPEN':
                    online_regions += 1

            if online_regions != len(split_keys):
                fail_count += 1
                time.sleep(3)

            if fail_count > 10:
                raise RuntimeError(
                    "when creating table, all the regions are not online after 30s. check your hbase instance. ")

    def get_region_in_state_count(self, ns: bytes, tb: bytes, target_state: str, timeout=10):
        region_states = self.get_region_states(ns, tb)
        start = time.time()
        count = 0
        while count != len(region_states):
            count = 0
            for region_state in region_states.values():
                if region_state == target_state:
                    count += 1
            if count == len(region_states):
                break
            else:
                region_states = self.get_region_states(ns, tb)
            now = time.time()
            if now - start > timeout:
                raise TimeoutError("wait regions in state {} timeout {}s".format(target_state, timeout))
        return count

    def get_region_states(self, ns: bytes, tb: bytes):
        """
        Returns a map that encoded region name to region state ('OPEN', 'CLOSED')
        """
        # Construct a ScanRequest to retrieve region states for the given namespace and table
        rq = ScanRequest()
        if ns is None or len(ns) == 0:
            rq.scan.start_row = tb + b","
        else:
            rq.scan.start_row = ns + b':' + tb + b","
        rq.scan.column.append(Column(family="info".encode("utf-8")))
        rq.number_of_rows = 1000  # Adjust the number of rows as needed
        rq.region.type = 1
        rq.region.value = "hbase:meta,,1".encode('utf-8')

        # Send the scan request to the meta region server
        scan_resp: ScanResponse = self.meta_conn.send_request(rq, "Scan")

        region_states = {}
        # Process the scan response to extract region states
        for result in scan_resp.results:
            region_info = Region.from_cells(result.cell)
            region_name = region_info.get_region_name()
            region_state = None
            for cell in result.cell:
                if cell.qualifier == b'state':
                    region_state = cell.value.decode('utf-8')
                    break
            region_states[region_name] = region_state
        return region_states

    def delete_table(self, ns: bytes, tb: bytes):
        self.master_conn.delete_table(ns, tb)

    def disable_table(self, ns: bytes, tb: bytes):
        self.master_conn.disable_table(ns, tb)
        count = self.get_region_in_state_count(ns, tb, "CLOSED")

    def enable_table(self, ns: bytes, tb: bytes):
        self.master_conn.enable_table(ns, tb)

    def get_table(self, ns, tb):
        return Table(self.zk_quorum, ns, tb, self.meta_conn)

    def describe_table(self, ns: bytes, tb: bytes):
        return self.master_conn.describe_table(ns, tb)
