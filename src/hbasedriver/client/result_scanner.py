from hbasedriver.model import Row
from hbasedriver.operations import Scan
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest, Column
from hbasedriver.protobuf_py.Filter_pb2 import Filter
from hbasedriver.region import Region


# ref to ClientScanner.java
class ResultScanner:
    def __init__(self, scan: 'Scan', table_name, cluster_connection):
        self.scanner_id = None
        self.cluster_cnn = cluster_connection
        self.table_name = table_name
        self.scan = scan
        # cache all the rows from last fetch.
        self.cache: list[Row] = []
        self.closed = False

        self.current_region = None

        self.last_result = None

    # we should retrieve the results when the client first call next.
    def __next__(self):
        # this first request to open the scanner.
        rq = ScanRequest()
        region: Region = self.locate_target_region(self.scan.start_row)

        rq.region.type = 1
        rq.region.value = region.region_encoded
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
