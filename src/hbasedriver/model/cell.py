import enum


class CellType(enum.Enum):
    PUT = 1
    DELETE = 2
    DELETE_FAMILY_VERSION = 3
    DELETE_COLUMN = 4
    DELETE_FAMILY = 5


class Cell:
    LATEST_TIMESTAMP = 0x7fffffffffffffff
    """
    the smallest unit in hbase.
    """

    def __init__(self, rowkey, family, qualifier=None, value=None, ts=None, cell_type=CellType.PUT):
        self.rowkey = rowkey
        self.family = family
        self.qualifier = qualifier
        self.value = value
        self.ts = Cell.LATEST_TIMESTAMP if ts is None else ts
        self.type = cell_type
