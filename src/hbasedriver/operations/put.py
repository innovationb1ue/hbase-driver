from collections import defaultdict

from hbasedriver.model import Cell, CellType


class Put:
    def __init__(self, rowkey: bytes):
        self.rowkey = rowkey
        self.family_cells: dict[bytes, list[Cell]] = defaultdict(list)

    def add_column(self, family: bytes, qualifier: bytes, value: bytes, ts: int = None):
        if ts is not None and ts < 0:
            raise ValueError("Timestamp cannot be negative. ts={}".format(ts))
        self.family_cells[family].append(Cell(self.rowkey, family, qualifier, value, ts, CellType.PUT))
        return self
