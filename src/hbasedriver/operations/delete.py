from collections import defaultdict

from hbasedriver.model.cell import Cell


class Delete:
    def __init__(self, rowkey: bytes):
        self.rowkey = rowkey
        self.family_cells: dict[bytes, list[Cell]] = defaultdict(list)

    # delete all versions of the specified column
    def add_column(self, family: bytes, qualifier: bytes, value: bytes):
        self.family_cells[family].append(Cell(self.rowkey, family, qualifier, value))
        return self

    # delete whole family
    def add_family(self, family: bytes):
        self.family_cells[family] = []
        return self
