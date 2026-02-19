from hbasedriver.exceptions.RemoteException import RemoteException
from hbasedriver.exceptions.TableExceptions import TableNotFoundException
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest, Column, ScanResponse
from hbasedriver.region import Region
from hbasedriver.regionserver import RsConnection


# encapsule the interaction with meta region server.
class MetaRsConnection(RsConnection):
    # locate the region with given rowkey and table name. (must be called on rs with meta region)

    # todo: make this return HRegionLocation
    def locate_region(self, ns, tb, rowkey) -> Region:
        rq = ScanRequest()
        # normalize inputs to bytes
        if isinstance(ns, str):
            ns = ns.encode('utf-8')
        if isinstance(tb, str):
            tb = tb.encode('utf-8')
        if isinstance(rowkey, str):
            rowkey = rowkey.encode('utf-8')
        if not ns or ns == b"default":
            # Default namespace: use "table,rowkey,"
            rq.scan.start_row = tb + b"," + rowkey + b","
        else:
            # Namespaced: use "ns:table,rowkey,"
            rq.scan.start_row = ns + b":" + tb + b"," + rowkey + b","

        rq.scan.column.append(Column(family=b"info"))
        rq.scan.reversed = True
        # Read several rows backwards to find the correct table/region; sometimes the immediate
        # previous row may belong to a different table due to how meta is organized.
        rq.number_of_rows = 10
        rq.region.type = 1
        rq.close_scanner = True

        # rq.renew = True
        # scan the meta region.
        rq.region.value = "hbase:meta,,1".encode('utf-8')
        scan_resp: ScanResponse = self.send_request(rq, "Scan")
        if len(scan_resp.results) == 0:
            raise TableNotFoundException("Table not found {}.{}".format(ns.decode(), tb.decode()))

        # normalize namespace for comparison (treat empty as 'default')
        ns_for_compare = ns or b"default"

        # iterate results and return the first region that matches the requested table
        for res in scan_resp.results:
            regioninfo = Region.from_cells(res.cell)
            if regioninfo.region_info.table_name.namespace == ns_for_compare and regioninfo.region_info.table_name.qualifier == tb:
                return regioninfo

        raise TableNotFoundException("Table not found {}.{}".format(ns.decode(), tb.decode()))
