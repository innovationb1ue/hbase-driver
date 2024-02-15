from hbasedriver.protobuf_py.Client_pb2 import ScanRequest, Column, ScanResponse
from hbasedriver.Connection import Connection
from hbasedriver.region import Region


class MetaRsConnection(Connection):
    def __init__(self):
        super().__init__("ClientService")

    # locate the region with given rowkey and table name. (must be called on rs with meta region)
    def locate_region(self, ns, tb, rowkey) -> Region:
        rq = ScanRequest()
        if ns is None or len(ns) == 0:
            rq.scan.start_row = "{},{},".format(tb, rowkey).encode('utf-8')
        else:
            rq.scan.start_row = "{}:{},{},".format(ns, tb, rowkey).encode('utf-8')
        rq.scan.column.append(Column(family="info".encode("utf-8")))
        rq.scan.reversed = True
        rq.number_of_rows = 1
        rq.region.type = 1
        rq.renew = True
        # scan the meta region.
        rq.region.value = "hbase:meta,,1".encode('utf-8')
        scan_resp: ScanResponse = self.send_request(rq, "Scan")

        rq2 = ScanRequest()
        rq2.scanner_id = scan_resp.scanner_id
        rq2.number_of_rows = 1
        rq2.close_scanner = True
        resp2 = self.send_request(rq2, "Scan")
        return Region.from_cells(resp2.results[0].cell)
