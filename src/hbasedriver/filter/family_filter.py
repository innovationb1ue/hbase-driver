"""
FamilyFilter - Filter based on column family name.

This filter is used to filter based on the column family. It takes an operator
(equal, greater, not equal, etc) and a byte[] comparator for the family
portion of a key.
"""
from typing import List

from hbasedriver.filter.binary_comparator import BinaryComparator
from hbasedriver.filter.byte_array_comparable import ByteArrayComparable
from hbasedriver.filter.compare_filter import CompareFilter
from hbasedriver.filter.compare_operator import CompareOperator
from hbasedriver.filter.filter import ReturnCode, Filter
from hbasedriver.model import Cell
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


class FamilyFilter(CompareFilter):
    """
    This filter is used to filter based on the column family.

    It takes an operator (equal, greater, not equal, etc) and a comparator
    for the column family portion of a key.
    """

    def __init__(self, op: CompareOperator, family_comparator: ByteArrayComparable):
        """
        Initialize FamilyFilter.

        :param op: CompareOperator to use
        :param family_comparator: Comparator for the family comparison
        """
        super().__init__(op, family_comparator)
        self._filter_out_row = False
        self._found_column = False

    def get_name(self) -> str:
        return "org.apache.hadoop.hbase.filter.FamilyFilter"

    def reset(self) -> None:
        """Reset filter state for new row."""
        self._filter_out_row = False
        self._found_column = False

    # Implement Filter abstract methods (snake_case)
    def filter_all_remaining(self) -> bool:
        """Check if all remaining should be filtered out."""
        return False

    def filter_row_key(self, cell: Cell) -> bool:
        """Filter based on row key - not used for FamilyFilter."""
        return False

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """Filter based on column family."""
        if self._filter_out_row:
            return ReturnCode.NEXT_ROW

        # Compare the family
        family = cell.family if cell.family else b''
        cmp_result = self.comparator.compare_to(family, 0, len(family))

        if self._should_filter(cmp_result):
            return ReturnCode.SKIP

        self._found_column = True
        return ReturnCode.INCLUDE

    def transform_cell(self, cell: Cell) -> Cell:
        """Transform cell - no transformation for FamilyFilter."""
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
        """Serialize the FamilyFilter to bytes."""
        builder = FilterProtos.FamilyFilter()
        builder.compare_filter.CopyFrom(self.convert())
        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'FamilyFilter':
        """Deserialize FamilyFilter from bytes."""
        proto = FilterProtos.FamilyFilter()
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

        return FamilyFilter(value_compare_op, value_comparator)

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check if serialized fields are equal."""
        if not isinstance(other, FamilyFilter):
            return False
        return self.op == other.op

    # Implement CompareFilter abstract methods (camelCase)
    def filterRowKey(self, cell: Cell) -> bool:
        return self.filter_row_key(cell)

    def compareRow(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        """Compare row - not used for FamilyFilter."""
        return False

    def compareFamily(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        """Compare family name."""
        family = cell.family if cell.family else b''
        cmp_result = comparator.compare_to(family)
        return not self._should_filter(cmp_result)

    def compareQualifier(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        """Compare qualifier - not used for FamilyFilter."""
        return False

    def compareValue(self, op: CompareOperator, comparator: ByteArrayComparable, cell: Cell) -> bool:
        """Compare value - not used for FamilyFilter."""
        return False
