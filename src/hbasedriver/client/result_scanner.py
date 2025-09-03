from hbasedriver.client.cluster_connection import ClusterConnection
from hbasedriver.model import Row
from hbasedriver.operations import Scan
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest, Column
from hbasedriver.protobuf_py.Filter_pb2 import Filter
from hbasedriver.region import Region


# ref to ClientScanner.java
class ResultScanner:
    def __init__(self, scan: 'Scan', table_name, cluster_connection):
        self.scanner_id = None
        self.cluster_cnn: 'ClusterConnection' = cluster_connection
        self.table_name = table_name
        self.scan = scan
        # cache all the rows from last fetch.
        self.cache: list[Row] = []
        self.closed = False

        self.current_region = None

        self.last_result = None

        self.rl = self.cluster_cnn.locate_region(table_name, self.scan.start_row)

    # we should retrieve the results when the client first call next.
    def __next__(self):
        # this first request to open the scanner.
        rq = ScanRequest()
        # todo: finish this, region encoded is not get yet.
        rq.region.type = 1
        rq.region.value = self.rl.locations[0].region_encoded
        rq.scan.include_start_row = self.scan.start_row_inclusive
        rq.scan.start_row = self.scan.start_row
        rq.scan.stop_row = self.scan.end_row
        rq.scan.include_stop_row = self.scan.end_row_inclusive
        if self.scan.filter is not None:
            rq.scan.filter = Filter(self.scan.filter.get_name(), self.scan.filter.to_byte_array())
        rq.number_of_rows = self.scan.limit
        rq.renew = True

        for family, qfs in self.scan.family_map.items():
            if len(qfs) == 0:
                rq.scan.column.append(Column(family=family))
            else:
                for qf in qfs:
                    rq.scan.column.append(Column(family=family, qualifier=qf))

        resp: ScanResponse = self.send_request(rq, 'Scan')
        scanner_id = resp.scanner_id

        rq2 = ScanRequest()
        rq2.scanner_id = self.scanner_id
        rq2.number_of_rows = self.scan.limit
        resp2: ScanResponse = self.rs_conn.send_request(rq2, "Scan")
        rows = []
        for result in resp2.results:
            row = Row.from_result(result)
            rows.append(row)
        if len(rows) == 0:
            raise StopIteration
        return rows

    def load_cache(self):
        if self.closed:
            return

    def __iter__(self):
        return self

# /**
# * This class has the logic for handling scanners for regions with and without replicas. 1. A scan
# * is attempted on the default (primary) region, or a specific region. 2. The scanner sends all the
# * RPCs to the default/specific region until it is done, or, there is a timeout on the
# * default/specific region (a timeout of zero is disallowed). 3. If there is a timeout in (2) above,
# * scanner(s) is opened on the non-default replica(s) only for Consistency.TIMELINE without specific
# * replica id specified. 4. The results from the first successful scanner are taken, and it is
# * stored which server returned the results. 5. The next RPCs are done on the above stored server
# * until it is done or there is a timeout, in which case, the other replicas are queried (as in (3)
# * above).
# */
