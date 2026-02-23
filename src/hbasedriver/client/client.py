import time
from typing import TYPE_CHECKING

from hbasedriver import zk
from hbasedriver.client.admin import Admin
from hbasedriver.client.cluster_connection import ClusterConnection
from hbasedriver.client.table import Table
from hbasedriver.common.table_name import TableName
from hbasedriver.hbase_exceptions import (
    HBaseException,
    ConnectionException,
    ZooKeeperException,
    TableNotFoundException,
    TableDisabledException,
    RegionException,
    SerializationException,
    TimeoutException,
    ValidationException,
    FilterException,
    BatchException,
)
from hbasedriver.master import MasterConnection
from hbasedriver.meta_server import MetaRsConnection
from hbasedriver.operations import Get
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest, Column, ScanResponse
from hbasedriver.protobuf_py.HBase_pb2 import TableState
from hbasedriver.region import Region
from hbasedriver.validation import validate_config
from hbasedriver.zk import locate_meta_region
from hbasedriver.hbase_exceptions import (
    HBaseException,
    ConnectionException,
    ZooKeeperException,
    TableNotFoundException,
    TableDisabledException,
    RegionException,
    SerializationException,
    TimeoutException,
    ValidationException,
    FilterException,
    BatchException,
)

if TYPE_CHECKING:
    from hbasedriver.model import ColumnFamilyDescriptor


class Client:
    """Main client class for connecting to and interacting with HBase.

    The Client class acts like the HBase Java Connection interface.
    It provides access to Admin (for DDL operations), Table (for DML operations),
    and region metadata.

    Example:
        >>> from hbasedriver.client.client import Client
        >>> from hbasedriver.hbase_constants import HConstants
        >>>
        >>> # Initialize with configuration
        >>> config = {HConstants.ZOOKEEPER_QUORUM: "localhost:2181"}
        >>> client = Client(config)
        >>>
        >>> # Use with context manager for automatic cleanup
        >>> with client.get_table(b"default", b"mytable") as table:
        ...     table.put(Put(b"row1").add_column(b"cf", b"col", b"value"))
        >>>
        >>> # Or use directly
        >>> table = client.get_table(b"default", b"mytable")
        >>> try:
        ...     # operations
        ...     pass
        ... finally:
        ...     table.close()
        >>> client.close()

    Attributes:
        conf: Configuration dictionary
        zk_quorum: List of ZooKeeper quorum addresses
        master_host: HBase master host
        master_port: HBase master port
        meta_host: HBase meta region host
        meta_port: HBase meta region port
        cluster_connection: Cluster connection for region location
        master_conn: Connection to HBase master
        meta_conn: Connection to HBase meta region server
    """

    def __init__(self, conf: dict) -> None:
        """Initialize a new HBase client.

        Args:
            conf: Configuration dictionary with the following keys:
                - hbase.zookeeper.quorum (required): ZooKeeper quorum addresses
                - hbase.connection.pool.size (optional): Max connections per pool
                - hbase.connection.idle.timeout (optional): Idle timeout in seconds

        Example:
            >>> config = {
            ...     "hbase.zookeeper.quorum": "localhost:2181",
            ...     "hbase.connection.pool.size": 20
            ... }
            >>> client = Client(config)
        """
        self.conf = conf

        # Note: Configuration validation available via validate_config() function
        # if needed for debugging. Invalid config values are ignored.

        self.zk_quorum: list[str] = conf.get("hbase.zookeeper.quorum").split(",")

        self.master_host: str
        self.master_port: int
        self.meta_host: str
        self.meta_port: int

        self.master_host, self.master_port = zk.locate_master(self.zk_quorum)
        self.meta_host, self.meta_port = zk.locate_meta_region(self.zk_quorum)

        self.cluster_connection: ClusterConnection = ClusterConnection(conf)

        self.master_conn: MasterConnection = MasterConnection().connect(self.master_host, self.master_port)
        self.meta_conn: MetaRsConnection = MetaRsConnection().connect(self.meta_host, self.meta_port)

    def get_admin(self) -> Admin:
        """Get an Admin instance for performing DDL operations.

        Returns:
            Admin instance for table management operations

        Example:
            >>> with client.get_admin() as admin:
            ...     if not admin.table_exists(TableName.value_of(b"mytable")):
            ...         # create table logic
            ...         pass
        """
        return Admin(self)

    def get_table(self, ns: bytes | None, tb: bytes) -> Table:
        """Get a Table instance for performing DML operations.

        Args:
            ns: Namespace (bytes). If None, uses "default" namespace
            tb: Table name (bytes)

        Returns:
            Table instance for data operations

        Example:
            >>> table = client.get_table(b"default", b"mytable")
            >>> # or use with context manager
            >>> with client.get_table(b"mytable") as table:
            ...     table.put(Put(b"row1").add_column(b"cf", b"col", b"value"))
        """
        table = Table(self.conf, ns or b"default", tb)
        # attach cluster connection so Table can locate regions via the cluster
        table.cluster_conn = self.cluster_connection
        return table

    def check_regions_online(self, ns: bytes, tb: bytes, split_keys: list[bytes]) -> None:
        """Wait for all regions of a table to come online.

        Args:
            ns: Namespace (bytes)
            tb: Table name (bytes)
            split_keys: List of split keys used when creating the table

        Raises:
            RuntimeError: If regions do not come online within timeout
        """
        time.sleep(1)
        attempts = 0
        expected = len(split_keys) or 1  # At least 1 region

        while attempts < 30:
            region_states = self.get_region_states(ns, tb)
            online = sum(1 for state in region_states.values() if state == "OPEN")
            if online >= expected:
                return
            time.sleep(3)
            attempts += 1

        raise RuntimeError("Timeout: Not all regions came online after table creation.")

    def get_table_state(self, ns: bytes, tb: bytes) -> 'TableState | None':
        """Get the logical table state.

        Returns the logical table state ('ENABLED', 'DISABLED', etc.) from
        hbase:meta using a direct Get. This does not reflect region-level
        state but the table metadata state.

        Args:
            ns: Namespace (bytes)
            tb: Table name (bytes)

        Returns:
            TableState if table exists, None otherwise

        Example:
            >>> state = client.get_table_state(b"default", b"mytable")
            >>> if state and state.state == TableState.ENABLED:
            ...     print("Table is enabled")
        """
        # Construct the logical table rowkey
        if not ns or ns == b"default":
            rowkey = tb
        else:
            rowkey = ns + b":" + tb

        get = Get(rowkey)
        get.add_column(b"table", b"state")

        result = self.meta_conn.get(b"hbase:meta,,1", get)

        # table does not exist.
        if result is None:
            return None

        val = result.get(b"table", b"state")
        state = TableState()
        state.ParseFromString(val)
        return state

    def get_region_states(self, ns: bytes, tb: bytes) -> dict[str, str]:
        """
        Returns a map from region encoded name to region state ('OPEN', 'CLOSED') for the given table only.
        Filters out unrelated table and non-region rows.
        """
        rq = ScanRequest()

        # Compute scan prefix
        if not ns or ns == b"default":
            prefix = tb
        else:
            prefix = ns + b":" + tb  # matches full table name in HBase

        # Compute stop row to avoid scanning other tables
        stop_row = bytearray(prefix)
        for i in reversed(range(len(stop_row))):
            if stop_row[i] != 0xFF:
                stop_row[i] += 1
                stop_row = stop_row[:i + 1]
                break
        else:
            stop_row.append(0x00)

        rq.scan.start_row = bytes(prefix)
        rq.scan.include_start_row = True;
        rq.scan.stop_row = bytes(stop_row)
        rq.scan.column.append(Column(family=b"info"))
        rq.number_of_rows = 1000
        rq.region.type = 1
        rq.region.value = b"hbase:meta,,1"

        scan_resp: ScanResponse = self.meta_conn.send_request(rq, "Scan")

        region_states = {}
        for result in scan_resp.results:
            # Skip table-level state entries (e.g., row key == table name with no region suffix)
            rowkey = result.cell[0].row
            if rowkey.startswith(prefix) and b',' in rowkey[len(prefix):]:
                # this is a region-level row
                region = Region.from_cells(result.cell)
                encoded_name = region.get_region_name()
                state = None
                for cell in result.cell:
                    if cell.family == b"info" and cell.qualifier == b"state":
                        state = cell.value.decode("utf-8")
                        break
                if state:
                    region_states[encoded_name] = state

        return region_states

    def get_region_in_state_count(self, ns: bytes, tb: bytes, target_state: str, timeout=60):
        start = time.time()
        while True:
            states = self.get_region_states(ns, tb)
            count = sum(1 for s in states.values() if s == target_state)
            if count == len(states):
                return count
            if time.time() - start > timeout:
                raise TimeoutError(f"Timeout waiting for all regions in state: {target_state}")
            time.sleep(1)

    def describe_table(self, ns: bytes, tb: bytes):
        return self.master_conn.describe_table(ns, tb)

    def create_table(
            self,
            ns: bytes,
            tb: bytes,
            column_families: 'list[ColumnFamilyDescriptor]',
            split_keys: list[bytes] | None = None
    ) -> None:
        tn = TableName.value_of(ns or b"", tb)
        return self.get_admin().create_table(tn, column_families, split_keys)

    def delete_table(self, ns: bytes, tb: bytes) -> None:
        tn = TableName.value_of(ns or b"", tb)
        return self.get_admin().delete_table(tn)

    def disable_table(self, ns: bytes, tb: bytes) -> None:
        tn = TableName.value_of(ns or b"", tb)
        return self.get_admin().disable_table(tn)

    def enable_table(self, ns: bytes, tb: bytes) -> None:
        tn = TableName.value_of(ns or b"", tb)
        return self.get_admin().enable_table(tn)

    def locate_region(self, table_name: TableName, row: bytes) -> tuple[str, int]:
        if table_name == TableName.META_TABLE_NAME:
            return locate_meta_region(self.zk_quorum)
        return self.meta_conn.locate_region(table_name.ns, table_name.tb, row)

    def __rebuild_connection(self) -> None:
        self.master_host, self.master_port = zk.locate_master(self.zk_quorum)
        self.meta_host, self.meta_port = zk.locate_meta_region(self.zk_quorum)
        self.master_conn = MasterConnection().connect(self.master_host, self.master_port)
        self.meta_conn = MetaRsConnection().connect(self.meta_host, self.meta_port)

    def __enter__(self):
        """Enter the context manager.

        Returns:
            The Client instance
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and close all resources.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred

        Returns:
            False - don't suppress any exceptions
        """
        self.close()
        return False

    def close(self):
        """Close the client and release all resources.

        This closes the cluster connection and cleans up any cached resources.
        """
        # Close cluster connection if exists
        if hasattr(self, 'cluster_connection') and self.cluster_connection:
            self.cluster_connection.close()

        # Close master and meta connections
        if hasattr(self, 'master_conn') and self.master_conn:
            try:
                self.master_conn.close()
            except Exception:
                pass

        if hasattr(self, 'meta_conn') and self.meta_conn:
            try:
                self.meta_conn.close()
            except Exception:
                pass
