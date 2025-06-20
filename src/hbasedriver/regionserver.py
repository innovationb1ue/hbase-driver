from hbasedriver.model import CellType
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.get import Get
from hbasedriver.operations.put import Put
from hbasedriver.operations.scan import Scan
from hbasedriver.protobuf_py.Client_pb2 import GetRequest, Column, ScanRequest, ScanResponse, MutateRequest, \
    MutationProto, MutateResponse
from hbasedriver.protobuf_py.Filter_pb2 import Filter
from hbasedriver.protobuf_py.HBase_pb2 import RegionLocation, RegionInfo, TimeRange

from hbasedriver.Connection import Connection
from hbasedriver.model.row import Row
from hbasedriver.region import Region


class RsConnection(Connection):
    def clone(self):
        if not self.host or not self.port:
            raise Exception("can not clone connection that is not properly initialized. ")
        new_conn = RsConnection().connect(self.host, self.port)
        return new_conn

    def __init__(self):
        super().__init__("ClientService")

    def put(self, region_name_encoded: bytes, put: Put) -> bool:
        # send put request to the target region and receive response(processed?)
        rq = MutateRequest()
        # set target region
        rq.region.type = 1
        rq.region.value = region_name_encoded
        # set kv pairs
        rq.mutation.mutate_type = MutationProto.MutationType.PUT
        rq.mutation.row = put.rowkey
        for family, cells in put.family_cells.items():
            col = MutationProto.ColumnValue(family=family)
            for cell in cells:
                col.qualifier_value.append(
                    MutationProto.ColumnValue.QualifierValue(qualifier=cell.qualifier, value=cell.value,
                                                             timestamp=cell.ts))
            rq.mutation.column_value.append(col)
        resp: MutateResponse = self.send_request(rq, "Mutate")
        return resp.processed

    def get(self, region_name_encoded: bytes, get: Get) -> Row | None:
        rq = GetRequest()

        # Set region metadata
        rq.region.type = 1
        rq.region.value = region_name_encoded

        # Set Get fields
        rq.get.row = get.rowkey
        rq.get.max_versions = get.max_versions
        rq.get.cache_blocks = False
        rq.get.existence_only = get.check_existence_only

        # Time range (only `to` is available in protobuf)
        if get.time_ranges[1] != 0x7fffffffffffffff:
            rq.get.time_range = TimeRange(to=get.time_ranges[1])

        # Filter
        # if get.filter is not None:
        # rq.get.filter.CopyFrom(get.filter.to_protobuf())

        # Columns
        for family, qualifiers in get.family_columns.items():
            if not qualifiers:
                rq.get.column.append(Column(family=family))
            else:
                rq.get.column.append(Column(family=family, qualifier=qualifiers))

        resp = self.send_request(rq, "Get")
        return Row.from_result(resp.result)

    def delete(self, region, delete: Delete):
        """
        :param delete:
        :param region:
        :return:
        """
        rq = MutateRequest()
        rq.mutation.mutate_type = MutationProto.MutationType.DELETE
        rq.region.type = 1
        rq.region.value = region.region_encoded

        rq.mutation.row = delete.rowkey

        # add specifications if there is any.
        for cf, cells in delete.family_cells.items():
            col = MutationProto.ColumnValue(family=cf)

            # add any qualifier if provided.
            for cell in cells:
                # delete all columns in the family smaller than the provided timestamp.
                if cell.type == CellType.DELETE_FAMILY:
                    col.qualifier_value.append(
                        MutationProto.ColumnValue.QualifierValue(qualifier=cell.qualifier,
                                                                 delete_type=MutationProto.DELETE_FAMILY,
                                                                 timestamp=cell.ts)
                    )
                # delete target column with specified version.
                elif cell.type == CellType.DELETE:
                    col.qualifier_value.append(
                        MutationProto.ColumnValue.QualifierValue(qualifier=cell.qualifier
                                                                 , delete_type=MutationProto.DELETE_ONE_VERSION,
                                                                 timestamp=cell.ts)
                    )
                # Delete all versions of the specified column with a timestamp less than or equal to the specified timestamp.
                elif cell.type == CellType.DELETE_COLUMN:
                    col.qualifier_value.append(
                        MutationProto.ColumnValue.QualifierValue(qualifier=cell.qualifier,
                                                                 delete_type=MutationProto.DELETE_MULTIPLE_VERSIONS,
                                                                 timestamp=cell.ts)
                    )
                # Delete all columns of the specified family with a timestamp equal to the specified timestamp.
                elif cell.type == CellType.DELETE_FAMILY_VERSION:
                    col.qualifier_value.append(
                        MutationProto.ColumnValue.QualifierValue(qualifier=cell.qualifier,
                                                                 delete_type=MutationProto.DELETE_FAMILY_VERSION,
                                                                 timestamp=cell.ts)
                    )

            rq.mutation.column_value.append(col)

        resp: MutateResponse = self.send_request(rq, "Mutate")
        return resp.processed

    def scan(self, region: Region, scan: Scan) -> 'ResultScanner':
        # this first request to open the scanner.

        # todo: refactor this to only return a valid resultScanner.
        rq = ScanRequest()
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
        # build an iterator to let client iterate through the result set.
        return ResultScanner(scanner_id, scan, self)

    # todo: impl this like the java code.
    def get_scanner(self, scan: Scan) -> 'ResultScanner':
        pass
