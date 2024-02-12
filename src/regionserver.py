import socket
from struct import pack

from Client_pb2 import GetRequest, Column, ScanRequest, ScanResponse
from RPC_pb2 import ConnectionHeader
from src import zk
from src.Connection import Connection


class RsConnection(Connection):
    def __init__(self):
        super().__init__("ClientService")

    # locate the region with given rowkey and table name. (must be called on rs with meta region. )
    def locate_region(self, ns: str, tb: str, rowkey: str):
        rq = ScanRequest()
        if len(ns) == 0:
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

        return resp2

    def put(self, ns, table, rowkey, kvs):
        # 1. locate region (scan meta)
        # 2. send put request to that region and receive response?
        self.locate_region(ns, table, rowkey)
        pass
