class RegionName:
    """
    This is a holder for a region name, including a host and a port.
    we use this to identify a region.
    This is typically used in a PUT or DELETE request that need to send request to a specify region.
    """

    def __init__(self, region_encoded: bytes, host: bytes, port: bytes):
        self.region_encoded = region_encoded
        self.host = host
        self.port = port

    @staticmethod
    def from_cells(cells):
        for c in cells:
            qf = c.qualifier
            if qf == b"server":
                value = c.value
                row = c.row
                host, port = value.split(b":")
                return RegionName(row, host, port)
        else:
            raise Exception("failed to find region name from cells. ")
