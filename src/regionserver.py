import socket
from struct import pack

from Client_pb2 import GetRequest, Column, ScanRequest, ScanResponse, MutateRequest, MutationProto, MutateResponse
from RPC_pb2 import ConnectionHeader
from src import zk
from src.Connection import Connection
from src.region_name import RegionName
from src.util import to_bytes


class RsConnection(Connection):
    def __init__(self):
        super().__init__("ClientService")

    # locate the region with given rowkey and table name. (must be called on rs with meta region. )
    def locate_region(self, ns, tb, rowkey):
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

        return RegionName.from_cells(resp2.results[0].cell)

    def put(self, ns, table, rowkey, cf_to_qf_vals: dict):
        """
        :param ns:
        :param table:
        :param rowkey:
        :param cf_to_qf_vals: in the format of dict{"cf": {"qf1": "val1", "qf2": "val2"}, ...}
        :return:
        """
        # 1. locate region (scan meta)
        # 2. send put request to that region and receive response?
        region_name = self.locate_region(ns, table, rowkey)
        rq = MutateRequest()
        # set target region
        rq.region.type = 1
        rq.region.value = region_name.region_encoded
        # set kv pairs
        rq.mutation.mutate_type = MutationProto.MutationType.PUT
        rq.mutation.row = bytes(rowkey, "utf-8")
        for cf, qf_value_pairs in cf_to_qf_vals.items():
            col = MutationProto.ColumnValue(family=bytes(cf, 'utf-8'))
            for qf, val in qf_value_pairs.items():
                col.qualifier_value.append(
                    MutationProto.ColumnValue.QualifierValue(qualifier=bytes(qf, "utf-8"), value=bytes(val, 'utf-8')))
            rq.mutation.column_value.append(col)
        resp: MutateResponse = self.send_request(rq, "Mutate")
        if not resp.processed:
            print("Put is not processed. ")
            return False
        else:
            print("Put success")
            return True

    def get(self, ns, table, rowkey, cf_to_qfs: dict):
        # 1. locate region (scan meta)
        # 2. send put request to that region and receive response?
        region_name = self.locate_region(ns, table, rowkey)
        rq = GetRequest()
        # set target region
        rq.region.type = 1
        rq.region.value = region_name.region_encoded
        # rowkey
        rq.get.row = bytes(rowkey, 'utf-8')
        # cfs
        for cf, qfs in cf_to_qfs.items():
            # get all qualifiers
            if len(qfs) == 0:
                rq.get.column.append(Column(family=bytes(cf, 'utf-8')))
                continue
            # get target qualifiers only
            if type(qfs[0]) != bytes:
                qfs = to_bytes(qfs)
            col = Column(family=bytes(cf, 'utf-8'), qualifier=qfs)
            rq.get.column.append(col)

        resp = self.send_request(rq, "Get")
        return resp.result.cell
