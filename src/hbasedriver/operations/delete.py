from collections import defaultdict

from hbasedriver.model.cell import Cell, CellType


class Delete:
    def __init__(self, rowkey: bytes):
        self.rowkey = rowkey
        self.family_cells: dict[bytes, list[Cell]] = defaultdict(list)

    # Delete the specified version of the specified column.
    def add_column(self, family: bytes, qualifier: bytes, ts: int):
        self.family_cells[family].append(Cell(self.rowkey, family, qualifier, ts=ts, cell_type=CellType.DELETE))
        return self

    # Delete all versions of the specified column with a timestamp less than or equal to the
    # specified timestamp.
    def add_columns(self, family: bytes, qualifier: bytes, ts: int):
        self.family_cells[family].append(Cell(self.rowkey, family, qualifier, ts, cell_type=CellType.DELETE_COLUMN))

    # Delete all columns of the specified family with a timestamp equal to the specified timestamp.
    def add_family_version(self, family: bytes, ts: int = None):
        self.family_cells[family].append(Cell(self.rowkey, family, ts=ts, cell_type=CellType.DELETE_FAMILY_VERSION))
        return self

    # Delete all columns of the specified family with a timestamp less than or equal to the specified
    def add_family(self, family: bytes, ts: int = None):
        self.family_cells[family].append(Cell(self.rowkey, family, ts=ts, cell_type=CellType.DELETE_FAMILY))
        return self
