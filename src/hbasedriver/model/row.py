import logging
from collections import defaultdict

logger = logging.getLogger("Row")


class Row:
    """
    Represent a row of data in hbase, with the unique rowkey.
    """

    def __init__(self, rowkey: bytes, kv: dict, exists: bool = True):
        self.rowkey = rowkey
        self.kv = kv
        self._exists = exists

    @property
    def row(self):
        """Backward-compatible alias used by some tests: row -> rowkey"""
        return self.rowkey

    def exists(self) -> bool:
        """Check if this row exists.

        Returns:
            True if the row exists, False otherwise
        """
        return self._exists

    # get the value of target column, return None if not exist.
    def get(self, cf: bytes, qf: bytes):
        tmp = self.kv.get(cf)
        if tmp:
            return tmp.get(qf)
        else:
            return None

    @staticmethod
    def from_result(result):
        # Handle existence-only check
        if hasattr(result, 'exists') and result.exists:
            # For exists check, we may not have cells but the row exists
            if len(result.cell) == 0:
                return Row(b'', {}, exists=True)

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
        return "<Row: key={}, columns={}>".format(str(self.rowkey), self.kv)

    def __repr__(self):
        return self.__str__()
