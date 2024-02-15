from collections import defaultdict


class Get:
    def __init__(self, rowkey: bytes):
        self.rowkey = rowkey
        self.family_columns: dict[bytes, list[bytes]] = defaultdict(list)
        self.time_ranges = (None, None)

    def add_family(self, family: bytes):
        self.family_columns[family] = []
        return self

    def add_column(self, family: bytes, qualifier: bytes):
        self.family_columns[family].append(qualifier)
        return self

    def set_time_range(self, min_ts, max_ts):
        self.time_ranges = (min_ts, max_ts)
        return self
