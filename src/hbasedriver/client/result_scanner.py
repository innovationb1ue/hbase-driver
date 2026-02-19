from hbasedriver.client.cluster_connection import ClusterConnection
from hbasedriver.model import Row
from hbasedriver.operations import Scan
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest, Column
from hbasedriver.protobuf_py.Filter_pb2 import Filter
from hbasedriver.region import Region


# ref to ClientScanner.java
class ResultScanner:
    def __init__(self, scan: 'Scan', table_name, cluster_connection: 'ClusterConnection'):
        self.scanner_id = None
        self.cluster_cnn: 'ClusterConnection' = cluster_connection
        self.table_name = table_name
        self.scan = scan
        # cache all the rows from last fetch.
        self.cache: list[Row] = []
        self.closed = False

        self.current_region = None
        self.last_result = None

        # locate region via cluster connection
        if not self.cluster_cnn:
            raise Exception("ClusterConnection required for ResultScanner")
        self.rl = self.cluster_cnn.locate_region(table_name, self.scan.start_row)

        # establish a regionserver connection for fetching scanner results
        from hbasedriver.regionserver import RsConnection
        host = self.rl.host.decode('utf-8') if isinstance(self.rl.host, bytes) else self.rl.host
        port = int(self.rl.port.decode('utf-8')) if isinstance(self.rl.port, bytes) else int(self.rl.port)
        self.rs_conn: RsConnection = RsConnection().connect(host, port)

    def __iter__(self):
        return self

    def __next__(self):
        # open scanner if not yet opened
        if self.scanner_id is None:
            rq = ScanRequest()
            rq.region.type = 1
            rq.region.value = self.rl.region_encoded
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

            resp = self.rs_conn.send_request(rq, 'Scan')
            self.scanner_id = resp.scanner_id
            results = resp.results
        else:
            rq2 = ScanRequest()
            rq2.scanner_id = self.scanner_id
            rq2.number_of_rows = self.scan.limit
            resp2 = self.rs_conn.send_request(rq2, "Scan")
            results = resp2.results

        if not results or len(results) == 0:
            raise StopIteration

        rows = [Row.from_result(result) for result in results]
        return rows

    def load_cache(self):
        if self.closed:
            return

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
