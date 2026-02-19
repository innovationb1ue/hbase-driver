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

    def disable_table(self, table_name: TableName, timeout: int = 60):
        self.master.disable_table(table_name.ns, table_name.tb)
        # Wait for the logical table state to become DISABLED; regions may take longer to report CLOSED.
        start = time.time()
        while time.time() - start < timeout:
            state = self.client.get_table_state(table_name.ns, table_name.tb)
            if state is not None and state.state == TableState.DISABLED:
                return
            time.sleep(1)
        raise TimeoutError("Timeout waiting for table to become DISABLED")

    def enable_table(self, table_name: TableName, timeout: int = 60):
        self.master.enable_table(table_name.ns, table_name.tb)
        # Wait for logical table state to be ENABLED
        start = time.time()
        while time.time() - start < timeout:
            state = self.client.get_table_state(table_name.ns, table_name.tb)
            if state is not None and state.state == TableState.ENABLED:
                return
            time.sleep(1)
        raise TimeoutError("Timeout waiting for table to become ENABLED")

    def is_table_disabled(self, table_name: TableName) -> bool:
        state = self.client.get_table_state(table_name.ns, table_name.tb)
        return state is not None and state.state == TableState.DISABLED

    def is_table_enabled(self, table_name: TableName) -> bool:
        state = self.client.get_table_state(table_name.ns, table_name.tb)
        return state is not None and state.state == TableState.ENABLED

    def describe_table(self, table_name: TableName):
        return self.master.describe_table(table_name.ns, table_name.tb)

    def list_tables(self, pattern: str = ".*", include_sys_tables: bool = False):
        return self.master.list_table_descriptors(pattern, include_sys_tables)

    def create_namespace(self, namespace):
        """Create a namespace (accepts bytes or str)."""
        return self.master.create_namespace(namespace)

    def delete_namespace(self, namespace):
        """Delete a namespace (accepts bytes or str)."""
        return self.master.delete_namespace(namespace)

    def list_namespaces(self) -> list:
        """List namespaces; returns list of namespace names (strings)."""
        return self.master.list_namespaces()

    def close(self):
        # Optional cleanup logic
        pass
