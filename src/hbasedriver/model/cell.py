class Cell:
    """
    the smallest unit in hbase.
    """

    def __init__(self, rowkey, family, qualifier, value, ts=None):
        self.rowkey = rowkey
        self.family = family
        self.qualifier = qualifier
        self.value = value
        self.ts = ts
