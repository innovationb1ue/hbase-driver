from collections import defaultdict
from typing import Optional

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
        self.filter: Optional[Filter] = None

        # Java-parity scan options
        self.caching: int = 20
        self.batch_size: int = 0
        self.max_result_size: int = 0
        self.reversed: bool = False
        self.read_type = None
        self.need_cursor_result: bool = False
        self.allow_partial_results: bool = False
        self.small: bool = False
        self.mvcc_read_point: int = 0
        self.consistency = 0

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
        return self

    def add_column(self, family: bytes, qualifier: bytes):
        self.family_map[family].append(qualifier)
        return self

    def set_time_range(self, min_stamp, max_stamp):
        self.min_stamp = min_stamp
        self.max_stamp = max_stamp
        return self

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
        return self

    def set_filter(self, filter_in: Filter):
        self.filter = filter_in
        return self

    # Java-style parity setters
    def setCaching(self, caching: int):
        self.caching = caching
        return self

    def setBatch(self, batch_size: int):
        self.batch_size = batch_size
        return self

    def setMaxResultSize(self, size: int):
        self.max_result_size = size
        return self

    def setReversed(self, reversed_: bool):
        self.reversed = reversed_
        return self

    def setReadType(self, read_type):
        self.read_type = read_type
        return self

    def setNeedCursorResult(self, val: bool):
        self.need_cursor_result = val
        return self

    def setAllowPartialResults(self, val: bool):
        self.allow_partial_results = val
        return self

    def setSmall(self, val: bool):
        self.small = val
        return self

    def setConsistency(self, consistency):
        self.consistency = consistency
        return self

    def to_protobuf(self):
        """Create and return a protobuf Scan message populated from this Scan."""
        from hbasedriver.protobuf_py.Client_pb2 import Scan as PbScan, Column as PbColumn
        from hbasedriver.protobuf_py.Filter_pb2 import Filter as PbFilter

        pb_scan = PbScan()
        pb_scan.include_start_row = self.start_row_inclusive
        pb_scan.start_row = self.start_row
        pb_scan.stop_row = self.end_row
        pb_scan.include_stop_row = self.end_row_inclusive

        if self.filter is not None:
            # protobuf Filter can be constructed from name + serialized bytes
            pb_scan.filter = PbFilter(self.filter.get_name(), self.filter.to_byte_array())

        if self.batch_size:
            pb_scan.batch_size = self.batch_size
        if self.max_result_size:
            pb_scan.max_result_size = self.max_result_size
        if self.caching:
            pb_scan.caching = self.caching

        pb_scan.reversed = self.reversed
        if self.read_type is not None:
            pb_scan.readType = self.read_type

        pb_scan.need_cursor_result = self.need_cursor_result
        pb_scan.allow_partial_results = self.allow_partial_results
        pb_scan.small = self.small

        if self.mvcc_read_point:
            pb_scan.mvcc_read_point = self.mvcc_read_point
        if self.consistency is not None:
            pb_scan.consistency = self.consistency

        for family, qfs in self.family_map.items():
            if len(qfs) == 0:
                pb_scan.column.append(PbColumn(family=family))
            else:
                for qf in qfs:
                    pb_scan.column.append(PbColumn(family=family, qualifier=qf))

        return pb_scan
