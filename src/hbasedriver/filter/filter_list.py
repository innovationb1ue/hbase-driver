"""
FilterList - Combines multiple filters with AND/OR logic.

This filter is used to combine multiple filters with MUST_PASS_ALL (AND)
or MUST_PASS_ONE (OR) operators.
"""
from enum import Enum
from typing import List

from hbasedriver.filter.filter import Filter, ReturnCode
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.model import Cell
import hbasedriver.protobuf_py.Filter_pb2 as FilterProtos


class FilterList(FilterBase):
    """
    Implementation of HBase FilterList for combining multiple filters.

    Operator.MUST_PASS_ALL - Returns true only if all filters pass (AND logic)
    Operator.MUST_PASS_ONE - Returns true if any filter passes (OR logic)
    """

    class Operator(Enum):
        MUST_PASS_ALL = 1  # AND - all filters must pass
        MUST_PASS_ONE = 2  # OR - at least one filter must pass

    def __init__(self, operator: Operator = None, filters: List[Filter] = None):
        """
        Initialize FilterList.

        :param operator: The operator to use (MUST_PASS_ALL or MUST_PASS_ONE).
                        Defaults to MUST_PASS_ALL.
        :param filters: List of filters to combine. Can be None or empty.
        """
        super().__init__()
        self.operator = operator if operator else FilterList.Operator.MUST_PASS_ALL
        self.filters: List[Filter] = filters if filters else []
        self._filter_out_row = False

    def get_name(self) -> str:
        return "org.apache.hadoop.hbase.filter.FilterList"

    def add_filter(self, filter_: Filter) -> None:
        """Add a filter to the list."""
        self.filters.append(filter_)

    def add_all(self, filters: List[Filter]) -> None:
        """Add multiple filters to the list."""
        self.filters.extend(filters)

    def get_filters(self) -> List[Filter]:
        """Get the list of filters."""
        return self.filters

    def get_operator(self) -> Operator:
        """Get the operator."""
        return self.operator

    def set_operator(self, operator: Operator) -> None:
        """Set the operator."""
        self.operator = operator

    def reset(self) -> None:
        """Reset all filters in the list."""
        self._filter_out_row = False
        for f in self.filters:
            f.reset()

    # Implement Filter abstract methods (snake_case)
    def filter_all_remaining(self) -> bool:
        """
        Check if all remaining should be filtered out.

        For MUST_PASS_ALL: Returns true if any filter returns true.
        For MUST_PASS_ONE: Returns true if all filters return true.
        """
        for f in self.filters:
            result = f.filter_all_remaining()
            if self.operator == FilterList.Operator.MUST_PASS_ALL:
                if result:
                    return True
            else:  # MUST_PASS_ONE
                if not result:
                    return False
        return self.operator == FilterList.Operator.MUST_PASS_ONE

    def filter_row_key(self, cell: Cell) -> bool:
        """
        Filter based on row key.

        For MUST_PASS_ALL: Returns true if any filter returns true.
        For MUST_PASS_ONE: Returns true if all filters return true.
        """
        for f in self.filters:
            result = f.filter_row_key(cell)
            if self.operator == FilterList.Operator.MUST_PASS_ALL:
                if result:
                    self._filter_out_row = True
                    return True
            else:  # MUST_PASS_ONE
                if not result:
                    return False
        if self.operator == FilterList.Operator.MUST_PASS_ONE:
            return False
        return self._filter_out_row

    def filter_cell(self, cell: Cell) -> ReturnCode:
        """
        Filter a cell based on the combined filter logic.

        For MUST_PASS_ALL: Returns the most restrictive ReturnCode.
        For MUST_PASS_ONE: Returns the least restrictive ReturnCode.
        """
        if not self.filters:
            return ReturnCode.INCLUDE

        rc = ReturnCode.INCLUDE if self.operator == FilterList.Operator.MUST_PASS_ONE else None

        for f in self.filters:
            filter_rc = f.filter_cell(cell)
            if self.operator == FilterList.Operator.MUST_PASS_ALL:
                if rc is None:
                    rc = filter_rc
                elif filter_rc == ReturnCode.NEXT_ROW or rc == ReturnCode.NEXT_ROW:
                    rc = ReturnCode.NEXT_ROW
                elif filter_rc == ReturnCode.SKIP or rc == ReturnCode.SKIP:
                    rc = ReturnCode.SKIP
            else:  # MUST_PASS_ONE
                if filter_rc == ReturnCode.INCLUDE:
                    return ReturnCode.INCLUDE
                elif rc is None or filter_rc.value < rc.value:
                    rc = filter_rc

        return rc if rc else ReturnCode.INCLUDE

    def transform_cell(self, cell: Cell) -> Cell:
        """Transform cell using the first filter that transforms it."""
        for f in self.filters:
            cell = f.transform_cell(cell)
        return cell

    def filter_row_cells(self, cells: List[Cell]) -> None:
        """Let each filter modify the cell list."""
        for f in self.filters:
            f.filter_row_cells(cells)

    def has_filter_row(self) -> bool:
        """Check if any filter has filter row functionality."""
        for f in self.filters:
            if f.has_filter_row():
                return True
        return False

    def filter_row(self) -> bool:
        """
        Filter the row based on combined logic.

        For MUST_PASS_ALL: Returns true if any filter returns true.
        For MUST_PASS_ONE: Returns true if all filters return true.
        """
        for f in self.filters:
            result = f.filter_row()
            if self.operator == FilterList.Operator.MUST_PASS_ALL:
                if result:
                    return True
            else:  # MUST_PASS_ONE
                if not result:
                    return False
        return self.operator == FilterList.Operator.MUST_PASS_ONE

    def get_next_cell_hint(self, cell: Cell) -> Cell:
        """Get the next cell hint from the most restrictive filter."""
        hint = None
        for f in self.filters:
            current_hint = f.get_next_cell_hint(cell)
            if current_hint is not None:
                if hint is None:
                    hint = current_hint
                # Could compare hints here if needed
        return hint

    def is_family_essential(self, name: bytes) -> bool:
        """Check if family is essential for any filter."""
        for f in self.filters:
            if f.is_family_essential(name):
                return True
        return False

    def to_byte_array(self) -> bytes:
        """Serialize the FilterList to bytes."""
        builder = FilterProtos.FilterList()
        builder.operator = FilterProtos.FilterList.Operator.Value(self.operator.name)
        for f in self.filters:
            filter_proto = FilterProtos.Filter()
            filter_proto.name = f.get_name()
            filter_proto.serialized_filter = f.to_byte_array()
            builder.filters.append(filter_proto)
        return builder.SerializeToString()

    @staticmethod
    def parse_from(pb_bytes: bytes) -> 'FilterList':
        """Deserialize FilterList from bytes."""
        proto = FilterProtos.FilterList()
        proto.MergeFromString(pb_bytes)

        operator = FilterList.Operator[proto.operator.name]
        # Note: Parsing individual filters requires a filter registry
        # which is not implemented yet. The filters list will be empty.
        return FilterList(operator=operator, filters=[])

    def are_serialized_fields_equal(self, other: Filter) -> bool:
        """Check if serialized fields are equal."""
        if not isinstance(other, FilterList):
            return False
        if self.operator != other.operator:
            return False
        if len(self.filters) != len(other.filters):
            return False
        for i, f in enumerate(self.filters):
            if not f.are_serialized_fields_equal(other.filters[i]):
                return False
        return True

    def __str__(self) -> str:
        filter_strs = [str(f) for f in self.filters]
        return f"FilterList({self.operator.name}, [{', '.join(filter_strs)}])"
