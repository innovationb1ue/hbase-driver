from collections import defaultdict


class Scan:
    def __init__(self, start_row: bytes = None, end_row: bytes = None):
        self.start_row = start_row
        self.start_row_inclusive = True
        self.end_row = end_row
        self.end_row_inclusive = True
        self.min_stamp: int = 0
        self.max_stamp: int = 0x7fffffffffffffff
        self.family_map: dict[bytes, list] = defaultdict(list)

    # Get all columns from the specified family.
    def add_family(self, family: bytes):
        self.family_map[family] = []
        return self

    def add_column(self, family: bytes, qualifier: bytes):
        self.family_map[family].append(qualifier)
        return self

    def set_time_range(self, min_stamp, max_stamp):
        self.min_stamp = min_stamp
        self.max_stamp = max_stamp

    def with_start_row(self, start_row: bytes, inclusive=True):
        self.start_row = start_row
        self.start_row_inclusive = True
        return self
