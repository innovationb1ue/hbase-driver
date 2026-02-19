from hbasedriver.protobuf_py.HBase_pb2 import RegionInfo
from hbasedriver.util.bytes import to_string_binary


class Region:
    """
    This is a holder for an abstract region.
    we use this to identify a region.
    This is typically used in a PUT or DELETE request that need to send request to a specify region.
    """

    def __init__(self, region_encoded: bytes, host: bytes, port: bytes, region_info: RegionInfo):
        self.region_encoded = region_encoded
        self.host = host
        self.port = port
        self.region_info = region_info
        self.state = None

    def __str__(self):
        return "{},{},{}".format(self.host, self.port, to_string_binary(self.region_info.start_key))

    @staticmethod
    def from_cells(cells):
        row = None
        host = None
        port = None
        region_info = None
        for c in cells:
            qf = c.qualifier
            # record row from any cell
            row = c.row
            if qf == b"server":
                value = c.value
                try:
                    host, port = value.split(b":", 1)
                except Exception:
                    host = value
                    port = b""
            elif qf == b"regioninfo":
                region_info = RegionInfo()
                region_info.ParseFromString(c.value[4:])  # skip PBUF

        if region_info is None:
            raise AssertionError("missing regioninfo in meta cells")

        # host/port may be absent during transitions; default to empty bytes
        host = host or b""
        port = port or b""

        return Region(row, host, port, region_info)

    def key_in_region(self, rowkey: bytes):
        """
        This checks the provided row key belong to this region.
        """
        return (self.region_info.start_key <= rowkey
                and (rowkey < self.region_info.end_key or self.region_info.end_key == b''))

    def get_region_name(self) -> bytes:
        return self.region_encoded
