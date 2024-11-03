class TableName:
    # placeholder for static attributes. 
    META_TABLE_NAME = None

    def __init__(self, ns: bytes, tb: bytes):
        if not ns:
            self.ns = b'default'
        else:
            self.ns = ns
        self.tb = tb
        if len(tb) == 0:
            raise Exception("table name must not be empty.")


TableName.META_TABLE_NAME = TableName(b'hbase', b'meta')