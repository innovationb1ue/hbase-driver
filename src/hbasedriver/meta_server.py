from hbasedriver.exceptions.RemoteException import RemoteException
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest, Column, ScanResponse
from hbasedriver.region import Region
from hbasedriver.regionserver import RsConnection


# encapsule the interaction with meta region server.
class MetaRsConnection(RsConnection):
    # locate the region with given rowkey and table name. (must be called on rs with meta region)
    def locate_region(self, ns, tb, rowkey) -> Region:
        rq = ScanRequest()
        if not ns or ns == b"default":
            # Default namespace: use "table,rowkey,"
            rq.scan.start_row = tb + b"," + rowkey + b","
        else:
            # Namespaced: use "ns:table,rowkey,"
            rq.scan.start_row = ns + b":" + tb + b"," + rowkey + b","

        rq.scan.column.append(Column(family=b"info"))
        rq.scan.reversed = True
        rq.number_of_rows = 1
        rq.region.type = 1
        rq.close_scanner = True

        # rq.renew = True
        # scan the meta region.
        rq.region.value = "hbase:meta,,1".encode('utf-8')
        scan_resp: ScanResponse = self.send_request(rq, "Scan")
        if len(scan_resp.results) == 0:
            raise RemoteException("Table not found {}.{}".format(ns, tb))
        return Region.from_cells(scan_resp.results[0].cell)
