"""
SingleColumnValueFilter - Filter rows based on a specific column's value.

This filter evaluates the value of a single column and decides whether
to include or exclude the entire row based on that value.
"""
from typing import List, Optional

from hbasedriver.filter.binary_comparator import BinaryComparator
from hbasedriver.filter.byte_array_comparable import ByteArrayComparable
from hbasedriver.filter.compare_operator import CompareOperator
from hbasedriver.filter.filter import ReturnCode, Filter
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.model import Cell, CellType
from hbasedriver.protobuf_py.HBase_pb2 import CompareType
from hbasedriver.protobuf_py.Comparator_pb2 import Comparator as ComparatorProto
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


class SingleColumnValueFilter(FilterBase):
    """Filter entire rows based on value of a specific column."""

    def __init__(
        self,
        column_family: bytes,
        column_qualifier: bytes,
        compare_operator: CompareOperator,
        comparator: ByteArrayComparable,
        filter_if_missing: bool = False,
        latest_version_only: bool = False
    ):
        """
        Initialize SingleColumnValueFilter.

        Args:
            column_family: Column family name
            column_qualifier: Column qualifier name
            compare_operator: Comparison operator
            comparator: Value to compare against
            filter_if_missing: If True, filter rows missing the column
            latest_version_only: If True, only check latest version
        """
        super().__init__()
        self.column_family = column_family
        self.column_qualifier = column_qualifier
        self.compare_operator = compare_operator
        self.comparator = comparator
        self.filter_if_missing = filter_if_missing
        self.latest_version_only = latest_version_only
        self._found_match = False
        self._found_column = False

    def get_name(self) -> str:
        return "org.apache.hadoop.hbase.filter.SingleColumnValueFilter"

    def get_column_family(self) -> bytes:
        """Get the column family."""
        return self.column_family

    def get_column_qualifier(self) -> bytes:
        """Get the column qualifier."""
        return self.column_qualifier

    def get_compare_operator(self) -> CompareOperator:
        """Get the compare operator."""
        return self.compare_operator

    def get_comparator(self) -> ByteArrayComparable:
        """Get the comparator."""
        return self.comparator

    def get_filter_if_missing(self) -> bool:
        """Get the filter_if_missing flag."""
        return self.filter_if_missing

    def get_latest_version_only(self) -> bool:
        """Get the latest_version_only flag."""
        return self.latest_version_only

    def reset(self) -> None:
        """Reset filter state for new row."""
        self._found_match = False
        self._found_column = False

    # Implement Filter abstract methods (snake_case)
    def filter_all_remaining(self) -> bool:
        """Check if all remaining should be filtered out."""
        return False

    def filter_row_key(self, cell: Cell) -> bool:
        """SingleColumnValueFilter doesn't filter by row key."""
        return False

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """Evaluate each cell and track match."""
        # Check if this is our target column
        if cell.family != self.column_family or cell.qualifier != self.column_qualifier:
            return ReturnCode.INCLUDE

        self._found_column = True

        # Skip if we want latest version only and this isn't a PUT (which is latest)
        # In HBase, only PUT cells represent actual data; other types are markers
        if self.latest_version_only and cell.type != CellType.PUT:
            return ReturnCode.SKIP

        # Perform comparison
        value = cell.value if cell.value else b''
        compare_result = self.comparator.compare_to(value, 0, len(value))

        # Compare using the operator
        if self.compare_operator == CompareOperator.LESS:
            match = compare_result <= 0
        elif self.compare_operator == CompareOperator.LESS_OR_EQUAL:
            match = compare_result < 0
        elif self.compare_operator == CompareOperator.EQUAL:
            match = compare_result == 0
        elif self.compare_operator == CompareOperator.NOT_EQUAL:
            match = compare_result != 0
        elif self.compare_operator == CompareOperator.GREATER_OR_EQUAL:
            match = compare_result > 0
        elif self.compare_operator == CompareOperator.GREATER:
            match = compare_result >= 0
        else:
            match = False

        if match:
            self._found_match = True
            return ReturnCode.INCLUDE
        else:
            return ReturnCode.SKIP

    def transform_cell(self, cell: Cell) -> Cell:
        """No transformation."""
        return cell

    def filter_row_cells(self, cells: List[Cell]) -> None:
        """No direct cell list modification."""
        pass

    def has_filter_row(self) -> bool:
        """Check if filter has filter row functionality."""
        return True

    def filter_row(self) -> bool:
        """Filter row based on column match."""
        # If column was not found and filter_if_missing is True, filter out
        if not self._found_column and self.filter_if_missing:
            return True
        return not self._found_match

    def get_next_cell_hint(self, cell: Cell) -> Cell:
        """No hint."""
        return None

    def is_family_essential(self, name: bytes) -> bool:
        """Target column family is essential."""
        return name == self.column_family

    def to_byte_array(self) -> bytes:
        """Serialize to protobuf."""
        builder = FilterProtos.SingleColumnValueFilter()

        # SingleColumnValueFilter has compare_op and comparator fields directly (not nested)
        builder.compare_op = CompareType.Value(self.compare_operator.name)

        comparator_proto = ComparatorProto()
        comparator_proto.name = self.comparator.get_name()
        comparator_proto.serialized_comparator = self.comparator.to_byte_array()
        builder.comparator.CopyFrom(comparator_proto)

        builder.column_family = self.column_family
        builder.column_qualifier = self.column_qualifier
        builder.filter_if_missing = self.filter_if_missing
        builder.latest_version_only = self.latest_version_only

        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'SingleColumnValueFilter':
        """Deserialize from protobuf."""
        proto = FilterProtos.SingleColumnValueFilter()
        proto.MergeFromString(pb_bytes)

        # Extract compare_op and comparator info (direct fields, not nested)
        value_compare_op = CompareOperator(proto.compare_op)
        comparator = None

        if proto.HasField("comparator"):
            java_class_name = proto.comparator.name
            if java_class_name == "org.apache.hadoop.hbase.filter.BinaryComparator":
                comparator = BinaryComparator(
                    proto.comparator.serialized_comparator
                )

        return SingleColumnValueFilter(
            column_family=proto.column_family,
            column_qualifier=proto.column_qualifier,
            compare_operator=value_compare_op,
            comparator=comparator,
            filter_if_missing=proto.filter_if_missing,
            latest_version_only=proto.latest_version_only
        )

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check if serialized fields are equal."""
        if not isinstance(other, SingleColumnValueFilter):
            return False
        return (
            self.column_family == other.column_family and
            self.column_qualifier == other.column_qualifier and
            self.compare_operator == other.compare_operator and
            self.filter_if_missing == other.filter_if_missing and
            self.latest_version_only == other.latest_version_only
        )

    def __str__(self) -> str:
        return f"SingleColumnValueFilter({self.column_family}:{self.column_qualifier})"
