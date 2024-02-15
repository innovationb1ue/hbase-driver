from hbasedriver.protobuf_py.HBase_pb2 import RegionInfo


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

    @staticmethod
    def from_cells(cells):
        row, host, port, region_info = None, None, None, None
        for c in cells:
            qf = c.qualifier
            if qf == b"server":
                value = c.value
                row = c.row
                host, port = value.split(b":")
            elif qf == b"regioninfo":
                region_info = RegionInfo()
                region_info.ParseFromString(c.value[4:])  # skip PBUF

        assert host
        assert port
        assert region_info

        return Region(row, host, port, region_info)

    def key_in_region(self, rowkey: bytes):
        """
        This checks the provided row key belong to this region.
        """
        return self.region_info.start_key <= rowkey < self.region_info.end_key
