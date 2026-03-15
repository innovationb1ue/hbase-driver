from typing import TYPE_CHECKING

# deferred import of ResultScanner inside scan() to avoid circular import
from hbasedriver.model import CellType
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.get import Get
from hbasedriver.operations.put import Put
from hbasedriver.operations.scan import Scan
from hbasedriver.protobuf_py.Client_pb2 import (
    GetRequest, Column, ScanRequest, ScanResponse, MutateRequest,
    MutationProto, MutateResponse, Condition, MultiRequest, RegionAction,
    Action, MultiResponse
)
from hbasedriver.protobuf_py.Comparator_pb2 import (
    Comparator, ByteArrayComparable, BinaryComparator, NullComparator
)
from hbasedriver.protobuf_py.Filter_pb2 import Filter
from hbasedriver.protobuf_py.HBase_pb2 import RegionLocation, RegionInfo, TimeRange

from hbasedriver.Connection import Connection
from hbasedriver.model.row import Row
from hbasedriver.region import Region

if TYPE_CHECKING:
    from hbasedriver.client.result_scanner import ResultScanner


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

    def delete(self, region: Region, delete: Delete) -> bool:
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
        # Open scanner and return ResultScanner for the given region
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
        from hbasedriver.client.result_scanner import ResultScanner
        return ResultScanner(scanner_id, scan, self)

    def get_scanner(self, scan: Scan) -> 'ResultScanner':
        # This method is not currently implemented.
        # For scanning, use Table.scan() or Table.get_scanner() which provide
        # cluster-aware scanning that can span multiple regions.
        # The ResultScanner(scan, table_name, cluster_connection) constructor
        # provides the functionality referenced in the original TODO comment.
        raise NotImplementedError(
            "get_scanner() is not implemented. "
            "Use Table.scan() or Table.get_scanner() for scanning operations."
        )

    def check_and_mutate(
        self,
        region_name_encoded: bytes,
        rowkey: bytes,
        check_family: bytes,
        check_qualifier: bytes,
        check_value: bytes | None,
        compare_type: int,
        mutation_type: int,
        column_values: list[tuple[bytes, list[tuple[bytes, bytes]]]]
    ) -> tuple[bool, Row | None]:
        """Execute a conditional mutation (check-and-put or check-and-delete).

        This is a server-side atomic operation. If the condition is met,
        the mutation is performed atomically.

        Args:
            region_name_encoded: Encoded region name
            rowkey: Row key
            check_family: Column family to check
            check_qualifier: Column qualifier to check
            check_value: Expected value (None checks for non-existence)
            compare_type: CompareType value (EQUAL, NOT_EQUAL, etc.)
            mutation_type: MutationProto.MutationType value (PUT, DELETE, etc.)
            column_values: List of (family, [(qualifier, value), ...]) tuples

        Returns:
            Tuple of (success, result_row) where success indicates if the
            condition passed and mutation was performed, and result_row
            contains any returned result data.
        """
        rq = MutateRequest()
        rq.region.type = 1
        rq.region.value = region_name_encoded

        # Set up the condition
        rq.condition.row = rowkey
        rq.condition.family = check_family
        rq.condition.qualifier = check_qualifier
        rq.condition.compare_type = compare_type

        # Set up comparator for the expected value
        if check_value is not None:
            # Use BinaryComparator with ByteArrayComparable
            bac = ByteArrayComparable(value=check_value)
            bc = BinaryComparator(comparable=bac)
            rq.condition.comparator.name = "org.apache.hadoop.hbase.filter.BinaryComparator"
            rq.condition.comparator.serialized_comparator = bc.SerializeToString()
        else:
            # For null check, use a null comparator
            nc = NullComparator()
            rq.condition.comparator.name = "org.apache.hadoop.hbase.filter.NullComparator"
            rq.condition.comparator.serialized_comparator = nc.SerializeToString()

        # Set up the mutation
        rq.mutation.mutate_type = mutation_type
        rq.mutation.row = rowkey

        for family, qualifiers in column_values:
            col = MutationProto.ColumnValue(family=family)
            for qualifier, value in qualifiers:
                col.qualifier_value.append(
                    MutationProto.ColumnValue.QualifierValue(
                        qualifier=qualifier,
                        value=value
                    )
                )
            rq.mutation.column_value.append(col)

        resp = self.send_request(rq, "Mutate")
        result = None
        if resp.HasField('result'):
            result = Row.from_result(resp.result)
        return resp.processed, result

    def increment(
        self,
        region_name_encoded: bytes,
        rowkey: bytes,
        column_values: list[tuple[bytes, list[tuple[bytes, int]]]],
        return_results: bool = True
    ) -> Row | None:
        """Execute an atomic increment operation.

        This is a server-side atomic operation that increments counter values
        and returns the new values.

        Args:
            region_name_encoded: Encoded region name
            rowkey: Row key
            column_values: List of (family, [(qualifier, amount), ...]) tuples
                where amount is the integer to increment by
            return_results: Whether to return the incremented values

        Returns:
            Row containing the new counter values, or None if return_results is False
        """
        rq = MutateRequest()
        rq.region.type = 1
        rq.region.value = region_name_encoded

        # Set up the increment mutation
        rq.mutation.mutate_type = MutationProto.MutationType.INCREMENT
        rq.mutation.row = rowkey

        for family, qualifiers in column_values:
            col = MutationProto.ColumnValue(family=family)
            for qualifier, amount in qualifiers:
                # Encode increment amount as bytes (8-byte big-endian)
                amount_bytes = amount.to_bytes(8, byteorder='big', signed=True)
                col.qualifier_value.append(
                    MutationProto.ColumnValue.QualifierValue(
                        qualifier=qualifier,
                        value=amount_bytes
                    )
                )
            rq.mutation.column_value.append(col)

        resp: MutateResponse = self.send_request(rq, "Mutate")

        if return_results and resp.HasField('result'):
            return Row.from_result(resp.result)
        return None

    def append(
        self,
        region_name_encoded: bytes,
        rowkey: bytes,
        column_values: list[tuple[bytes, list[tuple[bytes, bytes]]]],
        return_results: bool = True
    ) -> Row | None:
        """Execute an atomic append operation.

        This is a server-side atomic operation that appends values to existing
        column values and returns the new values.

        Args:
            region_name_encoded: Encoded region name
            rowkey: Row key
            column_values: List of (family, [(qualifier, value_to_append), ...]) tuples
            return_results: Whether to return the appended values

        Returns:
            Row containing the new values after append, or None if return_results is False
        """
        rq = MutateRequest()
        rq.region.type = 1
        rq.region.value = region_name_encoded

        # Set up the append mutation
        rq.mutation.mutate_type = MutationProto.MutationType.APPEND
        rq.mutation.row = rowkey

        for family, qualifiers in column_values:
            col = MutationProto.ColumnValue(family=family)
            for qualifier, value in qualifiers:
                col.qualifier_value.append(
                    MutationProto.ColumnValue.QualifierValue(
                        qualifier=qualifier,
                        value=value
                    )
                )
            rq.mutation.column_value.append(col)

        resp = self.send_request(rq, "Mutate")

        if return_results and resp.HasField('result'):
            return Row.from_result(resp.result)
        return None

    def mutate_row(
        self,
        region_name_encoded: bytes,
        rowkey: bytes,
        mutations: list
    ) -> bool:
        """Execute multiple mutations atomically on a single row.

        This is a server-side atomic operation that combines multiple
        mutations (Put, Delete, Increment, Append) into a single atomic operation.

        Args:
            region_name_encoded: Encoded region name
            rowkey: Row key
            mutations: List of tuples (mutation_type, column_values)
                mutation_type: MutationProto.MutationType value
                column_values: list of (family, [(qualifier, value), ...])

        Returns:
            True if successful
        """
        rq = MultiRequest()
        region_action = RegionAction()
        region_action.region.type = 1
        region_action.region.value = region_name_encoded
        region_action.atomic = True

        for idx, (mutation_type, column_values) in enumerate(mutations):
            action = Action()
            action.index = idx

            mutation = MutationProto()
            mutation.mutate_type = mutation_type
            mutation.row = rowkey

            for family, qualifiers in column_values:
                col = MutationProto.ColumnValue(family=family)
                for qualifier, value in qualifiers:
                    if mutation_type == MutationProto.MutationType.INCREMENT:
                        # For increment, value is an integer amount encoded as 8-byte big-endian
                        amount_bytes = value.to_bytes(8, byteorder='big', signed=True)
                        col.qualifier_value.append(
                            MutationProto.ColumnValue.QualifierValue(
                                qualifier=qualifier,
                                value=amount_bytes
                            )
                        )
                    else:
                        col.qualifier_value.append(
                            MutationProto.ColumnValue.QualifierValue(
                                qualifier=qualifier,
                                value=value
                            )
                        )
                mutation.column_value.append(col)

            action.mutation.CopyFrom(mutation)
            region_action.action.append(action)

        rq.regionAction.append(region_action)

        resp = self.send_request(rq, "Multi")

        # Check for success
        if resp.regionActionResult:
            result = resp.regionActionResult[0]
            return result.HasField('processed') and result.processed
        return False
