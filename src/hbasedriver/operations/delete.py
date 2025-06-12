from collections import defaultdict

from hbasedriver.model.cell import Cell, CellType


class Delete:
    def __init__(self, rowkey: bytes):
        self.rowkey = rowkey
        self.family_cells: dict[bytes, list[Cell]] = defaultdict(list)
        self.LONG_MAX = 0x7fffffffffffffff

    def add_column(self, family: bytes, qualifier: bytes, ts: int = None):
        ts = self.LONG_MAX if ts is None else ts
        self.family_cells[family].append(
            Cell(self.rowkey, family, qualifier, ts=ts, cell_type=CellType.DELETE)
        )
        return self

    def add_columns(self, family: bytes, qualifier: bytes, ts: int):
        self.family_cells[family].append(
            Cell(self.rowkey, family, qualifier, ts=ts, cell_type=CellType.DELETE_COLUMN)
        )
        return self

    def add_family_version(self, family: bytes, ts: int = None):
        ts = self.LONG_MAX if ts is None else ts
        self.family_cells[family].append(
            Cell(self.rowkey, family, ts=ts, cell_type=CellType.DELETE_FAMILY_VERSION)
        )
        return self

    def add_family(self, family: bytes, ts: int = None):
        ts = self.LONG_MAX if ts is None else ts
        self.family_cells[family].append(
            Cell(self.rowkey, family, ts=ts, cell_type=CellType.DELETE_FAMILY)
        )
        return self

    def is_row_delete(self) -> bool:
        return not self.family_cells
