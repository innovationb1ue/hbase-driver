import time

from hbasedriver.common.table_name import TableName
from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema, TableState


class Admin:
    """
    Admin operations for managing tables in HBase.
    Mirrors the Java HBase Admin interface.
    """

    def __init__(self, client):
        self.client = client
        self.master = client.master_conn

    def table_exists(self, table_name: TableName) -> bool:
        try:
            schemas = self.master.describe_table(table_name.ns, table_name.tb)
            return len(schemas.table_schema) != 0
        except Exception:
            return False

    def create_table(self, table_name: TableName, column_families: list[ColumnFamilySchema],
                     split_keys: list[bytes] = None):
        self.master.create_table(table_name.ns, table_name.tb, column_families, split_keys)
        self.client.check_regions_online(table_name.ns, table_name.tb, split_keys or [])

    def delete_table(self, table_name: TableName):
        self.master.delete_table(table_name.ns, table_name.tb)
        time.sleep(1)

    def disable_table(self, table_name: TableName):
        self.master.disable_table(table_name.ns, table_name.tb)
        self.client.get_region_in_state_count(table_name.ns, table_name.tb, "CLOSED")

    def enable_table(self, table_name: TableName):
        self.master.enable_table(table_name.ns, table_name.tb)
        self.client.get_region_in_state_count(table_name.ns, table_name.tb, "OPEN")

    def is_table_disabled(self, table_name: TableName) -> bool:
        state = self.client.get_table_state(table_name.ns, table_name.tb)
        return state.state == TableState.DISABLED

    def is_table_enabled(self, table_name: TableName) -> bool:
        state = self.client.get_table_state(table_name.ns, table_name.tb)
        return state.state == TableState.ENABLED

    def describe_table(self, table_name: TableName):
        return self.master.describe_table(table_name.ns, table_name.tb)

    def list_tables(self, pattern: str = ".*", include_sys_tables: bool = False):
        return self.master.list_table_descriptors(pattern, include_sys_tables)

    def close(self):
        # Optional cleanup logic
        pass
