from collections import defaultdict

from hbasedriver.filter.filter import Filter


class Scan:
    MAX_ROWKEY_LENGTH = 32767

    def __init__(self, start_row: bytes = b'', end_row: bytes = b''):
        self.start_row = start_row
        self.start_row_inclusive = True
        self.end_row = end_row
        self.end_row_inclusive = True
        self.min_stamp: int = 0
        self.max_stamp: int = 0x7fffffffffffffff
        self.family_map: dict[bytes, list] = defaultdict(list)
        self.limit: int = 20
        self.filter: Filter | None = None

    # Get all columns from the specified family.
    def add_family(self, family: bytes):
        self.family_map[family] = []
        return self

    def set_limit(self, limit):
        """
        set the maximum of rows we want for this scan.
        :param limit:
        :return:
        """
        self.limit = limit

    def add_column(self, family: bytes, qualifier: bytes):
        self.family_map[family].append(qualifier)
        return self

    def set_time_range(self, min_stamp, max_stamp):
        self.min_stamp = min_stamp
        self.max_stamp = max_stamp

    def with_start_row(self, start_row: bytes, inclusive=True):
        if len(start_row) > Scan.MAX_ROWKEY_LENGTH:
            raise ValueError("rowkey length must be smaller than {}".format(Scan.MAX_ROWKEY_LENGTH))
        self.start_row = start_row
        self.start_row_inclusive = inclusive
        return self

    def with_end_row(self, end_row: bytes, inclusive=True):
        if len(end_row) > Scan.MAX_ROWKEY_LENGTH:
            raise ValueError("rowkey length must be smaller than {}".format(Scan.MAX_ROWKEY_LENGTH))
        self.end_row = end_row
        self.end_row_inclusive = inclusive

    def set_filter(self, filter_in: Filter):
        self.filter = filter_in
