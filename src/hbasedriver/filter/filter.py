# /**
#  * Interface for row and column filters directly applied within the regionserver. A filter can
#  * expect the following call sequence:
#  * <ul>
#  * <li>{@link #reset()} : reset the filter state before filtering a new row.</li>
#  * <li>{@link #filterAllRemaining()}: true means row scan is over; false means keep going.</li>
#  * <li>{@link #filterRowKey(Cell)}: true means drop this row; false means include.</li>
#  * <li>{@link #filterCell(Cell)}: decides whether to include or exclude this Cell. See
#  * {@link ReturnCode}.</li>
#  * <li>{@link #transformCell(Cell)}: if the Cell is included, let the filter transform the Cell.
#  * </li>
#  * <li>{@link #filterRowCells(List)}: allows direct modification of the final list to be submitted
#  * <li>{@link #filterRow()}: last chance to drop entire row based on the sequence of filter calls.
#  * Eg: filter a row if it doesn't contain a specified column.</li>
#  * </ul>
#  * Filter instances are created one per region/scan. This abstract class replaces the old
#  * RowFilterInterface. When implementing your own filters, consider inheriting {@link FilterBase} to
#  * help you reduce boilerplate.
#  * @see FilterBase
#  */
from abc import ABC, abstractmethod
from enum import Enum

from hbasedriver.filter.rowfilter import RowFilter


class Filter(ABC):
    def __init__(self):
        self.reversed = False

    @abstractmethod
    def get_name(self):
        """
        return java class full name
        """
        pass

    @abstractmethod
    def reset(self):
        """
        Reset the state of the filter between rows.
        """
        pass

    @abstractmethod
    def filter_all_remaining(self):
        """
        Determine if the row scan should be terminated.
        """
        pass

    @abstractmethod
    def filter_row_key(self, cell):
        """
        Filter a row based on the row key.
        """
        pass

    @abstractmethod
    def filter_cell(self, cell):
        """
        Filter a cell based on the column family, column qualifier, and/or the column value.
        """
        pass

    @abstractmethod
    def transform_cell(self, cell):
        """
        Allow the filter to transform the cell if included.
        """
        pass

    @abstractmethod
    def filter_row_cells(self, kvs):
        """
        Directly modify the final list of cells to be submitted.
        """
        pass

    @abstractmethod
    def has_filter_row(self):
        """
        Check if the filter actively uses filterRowCells(List) or filterRow().
        """
        pass

    @abstractmethod
    def filter_row(self):
        """
        Last chance to veto a row based on previous filterCell(Cell) calls.
        """
        pass

    @abstractmethod
    def get_next_cell_hint(self, current_cell):
        """
        Provide the next key to seek if the filter returns SEEK_NEXT_USING_HINT.
        """
        pass

    @abstractmethod
    def is_family_essential(self, name):
        """
        Check if the given column family is essential for the filter to check the row.
        """
        pass

    @abstractmethod
    def to_byte_array(self):
        """
        Serialize the filter.
        """
        pass

    @staticmethod
    @abstractmethod
    def parse_from(pb_bytes: bytes) -> RowFilter:
        """
        Deserialize the filter.
        """
        pass

    @abstractmethod
    def are_serialized_fields_equal(self, other):
        """
        Check if the serialized fields of the filter are equal.
        """
        pass

    def set_reversed(self, reversed_flag):
        """
        Alter the reversed scan flag.
        """
        self.reversed = reversed_flag

    def is_reversed(self):
        """
        Check if the scan is reversed.
        """
        return self.reversed


class ReturnCode(Enum):
    INCLUDE = 0
    INCLUDE_AND_NEXT_COL = 1
    SKIP = 2
    NEXT_COL = 3
    NEXT_ROW = 4
    SEEK_NEXT_USING_HINT = 5
    INCLUDE_AND_SEEK_NEXT_ROW = 6
