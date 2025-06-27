class TableName:
    def __init__(self, ns, tb):
        self.ns: bytes = ns
        self.tb: bytes = tb

    @staticmethod
    def value_of(ns, tb):
        if not ns:
            ns = b'default'
        if not tb:
            raise Exception("table name is empty.")
        return TableName(ns, tb)
