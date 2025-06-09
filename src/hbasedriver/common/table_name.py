class TableName:
    META_TABLE_NAME = None  # to be initialized after class definition

    def __init__(self, ns: bytes, tb: bytes):
        self.ns = ns or b'default'
        self.tb = tb
        if not tb:
            raise ValueError("Table name cannot be empty.")

    @staticmethod
    def value_of(ns: bytes, tb: bytes):
        return TableName(ns, tb)

    def get_namespace(self) -> bytes:
        return self.ns

    def get_qualifier(self) -> bytes:
        return self.tb

    def __eq__(self, other):
        if not isinstance(other, TableName):
            return False
        return self.ns == other.ns and self.tb == other.tb

    def __hash__(self):
        return hash((self.ns, self.tb))

    def __str__(self):
        return f"{self.ns.decode()}:{self.tb.decode()}"

    def __repr__(self):
        return f"TableName(ns={self.ns!r}, tb={self.tb!r})"


# Singleton for hbase:meta
TableName.META_TABLE_NAME = TableName(b'hbase', b'meta')
