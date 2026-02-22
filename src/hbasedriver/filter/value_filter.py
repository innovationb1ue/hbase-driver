"""
ValueFilter - Filter based on cell value.

This filter is used to filter based on the cell value. It takes an operator
(equal, greater, not equal, etc) and a byte[] comparator for the cell value.
"""
from typing import List

from hbasedriver.filter.binary_comparator import BinaryComparator
from hbasedriver.filter.byte_array_comparable import ByteArrayComparable
from hbasedriver.filter.compare_filter import CompareFilter
from hbasedriver.filter.compare_operator import CompareOperator
from hbasedriver.filter.filter import ReturnCode, Filter
from hbasedriver.model import Cell
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


class ValueFilter(CompareFilter):
    """
    This filter is used to filter based on column value.

    It takes an operator (equal, greater, not equal, etc) and a comparator
    for the cell value.
    """

    def __init__(self, op: CompareOperator, value_comparator: ByteArrayComparable):
        """
        Initialize ValueFilter.

        :param op: CompareOperator to use
        :param value_comparator: Comparator for the value comparison
        """
        super().__init__(op, value_comparator)
        self._found_column = False

    def get_name(self) -> str:
        return "org.apache.hadoop.hbase.filter.ValueFilter"

    def reset(self) -> None:
        """Reset filter state for new row."""
        self._found_column = False

    # Implement Filter abstract methods (snake_case)
    def filter_all_remaining(self) -> bool:
        """Check if all remaining should be filtered out."""
        return False

    def filter_row_key(self, cell: Cell) -> bool:
        """Filter based on row key - not used for ValueFilter."""
        return False

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """Filter based on cell value."""
        value = cell.value if cell.value else b''
        cmp_result = self.comparator.compare_to(value, 0, len(value))

        if self._should_filter(cmp_result):
            return ReturnCode.SKIP

        self._found_column = True
        return ReturnCode.INCLUDE

    def transform_cell(self, cell: Cell) -> Cell:
        """Transform cell - no transformation for ValueFilter."""
        return cell

    def filter_row_cells(self, cells: List[Cell]) -> None:
        """Direct modification of cell list - not used."""
        pass

    def has_filter_row(self) -> bool:
        """Check if filter has filter row functionality."""
        return True

    def filter_row(self) -> bool:
        """Filter out row if no matching column was found."""
        return not self._found_column

    def get_next_cell_hint(self, cell: Cell) -> Cell:
        """Get next cell hint - not used."""
        return None

    def is_family_essential(self, name: bytes) -> bool:
        """Check if family is essential."""
        return True

    def _should_filter(self, compare_result: int) -> bool:
        """
        Check if comparison result should be filtered out.

        Returns True if the cell should be filtered out (doesn't match).
        """
        if self.op == CompareOperator.EQUAL:
            return compare_result != 0
        elif self.op == CompareOperator.NOT_EQUAL:
            return compare_result == 0
        elif self.op == CompareOperator.LESS:
            return compare_result >= 0
        elif self.op == CompareOperator.LESS_OR_EQUAL:
            return compare_result > 0
        elif self.op == CompareOperator.GREATER:
            return compare_result <= 0
        elif self.op == CompareOperator.GREATER_OR_EQUAL:
            return compare_result < 0
        return True

    def to_byte_array(self) -> bytes:
        """Serialize the ValueFilter to bytes."""
        builder = FilterProtos.ValueFilter()
        builder.compare_filter.CopyFrom(self.convert())
        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'ValueFilter':
        """Deserialize ValueFilter from bytes."""
        proto = FilterProtos.ValueFilter()
        proto.MergeFromString(pb_bytes)

        value_compare_op = CompareOperator(proto.compare_filter.compare_op.Value())
        value_comparator = None

        if proto.compare_filter.HasField("comparator"):
            java_class_name = proto.compare_filter.comparator.name
            if java_class_name == "org.apache.hadoop.hbase.filter.BinaryComparator":
                value_comparator = BinaryComparator(
                    proto.compare_filter.comparator.serialized_comparator
                )
            else:
                raise NotImplementedError(
                    f"Comparator {java_class_name} is not supported yet."
                )

        return ValueFilter(value_compare_op, value_comparator)

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check if serialized fields are equal."""
        if not isinstance(other, ValueFilter):
            return False
        return self.op == other.op

    # Implement CompareFilter abstract methods (camelCase)
    def filterRowKey(self, cell: Cell) -> bool:
        return self.filter_row_key(cell)

    def compareRow(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        """Compare row - not used for ValueFilter."""
        return False

    def compareFamily(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        """Compare family - not used for ValueFilter."""
        return False

    def compareQualifier(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        """Compare qualifier - not used for ValueFilter."""
        return False

    def compareValue(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        """Compare cell value."""
        value = cell.value if cell.value else b''
        cmp_result = comparator.compare_to(value)
        return not self._should_filter(cmp_result)
