from collections import defaultdict


class Get:
    def __init__(self, rowkey: bytes):
        self.rowkey = rowkey
        self.family_columns: dict[bytes, list[bytes]] = defaultdict(list)
        self.time_ranges = (0, 0x7fffffffffffffff)
        self.max_versions = 1
        self.check_existence_only = False

    def add_family(self, family: bytes):
        self.family_columns[family] = []
        return self

    def add_column(self, family: bytes, qualifier: bytes):
        self.family_columns[family].append(qualifier)
        return self

    def set_time_range(self, min_ts, max_ts):
        self.time_ranges = (min_ts, max_ts)
        return self

    def set_time_stamp(self, ts):
        self.time_ranges = (ts, ts + 1)
        return self

    def read_versions(self, versions):
        if versions <= 0:
            raise Exception("versions must be positive")
        self.max_versions = versions

    def set_check_existence_only(self, check_existence_only):
        self.check_existence_only = check_existence_only
        return self
