from hbasedriver.hbase_exceptions import TableNotFoundException
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest, Column, ScanResponse
from hbasedriver.region import Region
from hbasedriver.regionserver import RsConnection
from hbasedriver.hregion_location import HRegionLocation


# encapsule the interaction with meta region server.
class MetaRsConnection(RsConnection):
    # locate the region with given rowkey and table name. (must be called on rs with meta region)
    def locate_region(self, ns, tb, rowkey) -> 'Region':
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
        # If caller provided empty rowkey, perform a forward bounded scan across the table's
        # meta entries to find any regions. This avoids relying solely on reversed scans that
        # sometimes return nothing due to propagation/order.
        if not rowkey:
            # scan from table prefix to table prefix + high-byte to capture all meta rows for this table
            if not ns or ns == b"default":
                rq.scan.start_row = tb + b","
            else:
                rq.scan.start_row = ns + b":" + tb + b","
            rq.scan.stop_row = rq.scan.start_row + b"\xff"
            rq.scan.reversed = False
            rq.number_of_rows = 100
        else:
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
        # If no results when scanning backwards, try a forward scan as a fallback. Some
        # meta propagation or ordering can make the reversed scan return nothing briefly.
        if len(scan_resp.results) == 0:
            # try forward scan
            rq.scan.reversed = False
            rq.number_of_rows = 10
            scan_resp = self.send_request(rq, "Scan")
            if len(scan_resp.results) == 0:
                raise TableNotFoundException("Table not found {}.{}".format(ns.decode(), tb.decode()))

        # normalize namespace for comparison (treat empty as 'default')
        ns_for_compare = ns or b"default"

        # iterate results and return the first region that matches the requested table
        for res in scan_resp.results:
            region = Region.from_cells(res.cell)
            if region.region_info.table_name.namespace == ns_for_compare and region.region_info.table_name.qualifier == tb:
                return region

        raise TableNotFoundException("Table not found {}.{}".format(ns.decode(), tb.decode()))
