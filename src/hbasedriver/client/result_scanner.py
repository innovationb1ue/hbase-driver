from hbasedriver.model import Row
from hbasedriver.operations import Scan
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest
from hbasedriver.region import Region


class ResultScanner:
    def __init__(self, scan: 'Scan', rs_conn: RsConnection):
        self.scanner_id = None
        self.rs_conn = rs_conn
        self.scan = scan
        # cache all the rows from last fetch.
        self.cache: list[Row] = []

    # we should retrieve the results when the client first call next.
    def __next__(self):
        # this first request to open the scanner.
        rq = ScanRequest()
        region: Region = self.locate_target_region(self.scan.start_row)

        rq.region.type = 1
        rq.region.value = region.region_encoded
        rq.scan.include_start_row = scan.start_row_inclusive
        rq.scan.start_row = scan.start_row
        rq.scan.stop_row = scan.end_row
        rq.scan.include_stop_row = scan.end_row_inclusive
        if scan.filter is not None:
            rq.scan.filter = Filter(scan.filter.get_name(), scan.filter.to_byte_array())
        rq.number_of_rows = scan.limit
        rq.renew = True
        for family, qfs in scan.family_map.items():
            rq.scan.column.append(Column(family=family))
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

    def __iter__(self):
        return self
