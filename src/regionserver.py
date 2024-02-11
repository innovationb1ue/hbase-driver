import socket
from struct import pack

from Client_pb2 import GetRequest, Column, ScanRequest
from RPC_pb2 import ConnectionHeader
from src.Connection import Connection


class RsConnection(Connection):
    def __init__(self):
        super().__init__("ClientService")

    def locate_region(self):
        rq = ScanRequest()
        rq.scan.start_row = "hbase:meta,,".encode('utf-8')
        rq.scan.column.append(Column(family="info".encode("utf-8")))
        rq.scan.reversed = True
        rq.region.type = 1
        rq.region.value = "hbase:meta,,1".encode('utf-8')

        get_resp = self.send_request(rq, "Scan")
        print(get_resp)
