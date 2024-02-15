import logging
from collections import defaultdict

logger = logging.getLogger("Row")


class Row:
    """
    Represent a row of data in hbase, with the unique rowkey.
    """

    def __init__(self, rowkey: bytes, kv: dict):
        self.rowkey = rowkey
        self.kv = kv

    # get the value of target column, return None if not exist.
    def get(self, cf: bytes, qf: bytes):
        tmp = self.kv.get(cf)
        if tmp:
            return tmp.get(qf)
        else:
            return None

    @staticmethod
    def from_result(result):
        # provide no cells, we return None here.
        if len(result.cell) == 0:
            return None
        kv = defaultdict(dict)
        for c in result.cell:
            kv[c.family][c.qualifier] = c.value
        return Row(result.cell[0].row, kv)

    @staticmethod
    def from_cell(cell):
        return Row(cell.row, {cell.family: {cell.family: cell.value}})

    def __str__(self):
        return "Row: key={}, columns={}".format(str(self.rowkey), self.kv)
