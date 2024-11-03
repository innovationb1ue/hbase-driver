from hbasedriver.common.table_name import TableName


class RegionLocator:
    def __init__(self, table_name: TableName):
        self.table_name = table_name
