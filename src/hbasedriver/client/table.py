import time
from typing import TYPE_CHECKING

from hbasedriver.client.result_scanner import ResultScanner
from hbasedriver.client.cluster_connection import ClusterConnection
from hbasedriver.connection.connection_pool import ConnectionPool
from hbasedriver.meta_server import MetaRsConnection
from hbasedriver.model.row import Row
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.get import Get
from hbasedriver.operations.scan import Scan
from hbasedriver.operations.append import Append
from hbasedriver.operations.row_mutations import RowMutations
from hbasedriver.region import Region
from hbasedriver.regionserver import RsConnection
from hbasedriver.table_name import TableName
from hbasedriver.zk import locate_meta_region
from hbasedriver.operations.put import Put
from hbasedriver.operations.batch import Batch, BatchGet, BatchPut
from hbasedriver.operations.increment import Increment, CheckAndPut
from hbasedriver.protobuf_py.HBase_pb2 import CompareType
from hbasedriver.protobuf_py.Client_pb2 import MutationProto

if TYPE_CHECKING:
    from hbasedriver.model import Row


class Table:
    """Table class for performing data operations on a specific HBase table.

    This class provides methods for Put, Get, Delete, Scan, Batch, CheckAndPut,
    and Increment operations on a specific table. Each Table instance maintains
    its own connection pool and region cache.

    Example:
        >>> from hbasedriver.operations.put import Put
        >>> from hbasedriver.operations.get import Get
        >>> from hbasedriver.operations.scan import Scan
        >>>
        >>> # Use with context manager
        >>> with client.get_table(b"default", b"mytable") as table:
        ...     # Put data
        ...     table.put(Put(b"row1").add_column(b"cf", b"col", b"value"))
        ...
        ...     # Get data
        ...     row = table.get(Get(b"row1"))
        ...     print(row.get(b"cf", b"col"))
        ...
        ...     # Scan data
        ...     with table.scan(Scan()) as scanner:
        ...         for row in scanner:
        ...             print(row)
        >>>
        >>> # After truncating a table, invalidate the region cache
        >>> admin.truncate_table(tn, preserve_splits=False)
        >>> table.invalidate_cache()  # Forces fresh region lookups

    Attributes:
        REGION_CACHE_TTL: TTL for region cache validation (seconds)
        ns: Namespace (bytes)
        tb: Table name (bytes)
        conf: Configuration dictionary
        regions: Cached regions with timestamp for TTL-based validation
        _conn_pool: Connection pool for reusing regionserver connections
        rs_conns: Deprecated: cached regionserver connections
        cluster_conn: Cluster connection for cross-region operations
    """

    # TTL for region cache validation (seconds)
    # Regions cached within this window skip meta validation
    REGION_CACHE_TTL = 60

    def __init__(self, conf: dict, ns: bytes | str, tb: bytes | str) -> None:
        # Ensure ns and tb are bytes
        if isinstance(ns, str):
            ns = ns.encode('utf-8')
        if isinstance(tb, str):
            tb = tb.encode('utf-8')

        self.ns: bytes = ns
        self.tb: bytes = tb
        self.conf: dict = conf
        self.meta_rs_host: str
        self.meta_rs_port: int
        self.meta_rs_host, self.meta_rs_port = locate_meta_region(conf.get("hbase.zookeeper.quorum").split(","))
        # cache metadata for regions that we touched, with timestamp for TTL-based validation
        # Format: {region_id: (region, timestamp)}
        self.regions: dict[int, tuple[Region, float]] = {}
        # Connection pool for reusing regionserver connections
        self._conn_pool = ConnectionPool(
            pool_size=conf.get('hbase.connection.pool.size', ConnectionPool.DEFAULT_POOL_SIZE),
            idle_timeout=conf.get('hbase.connection.idle.timeout', ConnectionPool.DEFAULT_IDLE_TIMEOUT)
        )
        # Deprecated: kept for backward compatibility, using connection pool now
        self.rs_conns: dict[tuple[bytes, int], RsConnection] = {}
        self.cluster_conn: ClusterConnection | None = None

    def put(self, put: Put) -> bool:
        """Insert or update a row in the table.

        Args:
            put: Put operation with row key and column values

        Returns:
            True if the put was successful

        Example:
            >>> put = Put(b"row1").add_column(b"cf", b"col", b"value")
            >>> table.put(put)
        """
        region: Region = self.locate_target_region(put.rowkey)
        conn = self.get_rs_connection(region)
        return conn.put(region.region_encoded, put)

    def get(self, get: Get) -> 'Row | None':
        """Retrieve a single row from the table.

        Args:
            get: Get operation specifying row key and columns

        Returns:
            Row object if found, None otherwise

        Example:
            >>> get = Get(b"row1").add_column(b"cf", b"col")
            >>> row = table.get(get)
            >>> if row:
            ...     print(row.get(b"cf", b"col"))
        """
        region: Region = self.locate_target_region(get.rowkey)
        conn = self.get_rs_connection(region)
        return conn.get(region.region_encoded, get)

    def exists(self, get: Get) -> bool:
        """Check if a row exists without fetching data.

        This is a server-side existence check that only checks if the row
        (or specified columns) exist without returning the actual data.

        Example:
            >>> # Check if entire row exists
            >>> get = Get(b"row1")
            >>> if table.exists(get):
            ...     print("Row exists")
            >>>
            >>> # Check if specific columns exist
            >>> get = Get(b"row2").add_column(b"cf", b"col")
            >>> if table.exists(get):
            ...     print("Column exists")

        Args:
            get: Get operation specifying row key and optionally columns to check

        Returns:
            True if row/columns exist, False otherwise
        """
        get.set_check_existence_only(True)
        region: Region = self.locate_target_region(get.rowkey)
        conn = self.get_rs_connection(region)
        row = conn.get(region.region_encoded, get)
        return row is not None and row.exists()

    def exists_all(self, gets: list[Get]) -> dict[bytes, bool]:
        """Check existence of multiple rows.

        This checks if each row (or specified columns) exists without
        fetching the actual data.

        Example:
            >>> gets = [Get(b"row1"), Get(b"row2"), Get(b"row3")]
            >>> results = table.exists_all(gets)
            >>> for rowkey, exists in results.items():
            ...     print(f"{rowkey}: {'exists' if exists else 'not found'}")

        Args:
            gets: List of Get operations specifying rows/columns to check

        Returns:
            Dictionary mapping row keys to existence boolean
        """
        results: dict[bytes, bool] = {}
        for get in gets:
            results[get.rowkey] = self.exists(get)
        return results

    def delete(self, delete: Delete) -> bool:
        """Delete a row or specific columns from the table.

        Args:
            delete: Delete operation specifying row key and columns to delete

        Returns:
            True if the delete was successful

        Example:
            >>> from hbasedriver.operations.delete import Delete
            >>> # Delete entire row
            >>> delete = Delete(b"row1")
            >>> table.delete(delete)
            >>> # Delete specific column
            >>> delete = Delete(b"row2").add_column(b"cf", b"col")
            >>> table.delete(delete)
        """
        region: Region = self.locate_target_region(delete.rowkey)
        conn = self.get_rs_connection(region)
        return conn.delete(region, delete)

    def scan(self, scan: Scan) -> ResultScanner:
        """Scan the table and return a ResultScanner.

        Args:
            scan: Scan operation specifying row range and filters

        Returns:
            ResultScanner for iterating through results

        Example:
            >>> scan = Scan().add_column(b"cf", b"col")
            >>> with table.scan(scan) as scanner:
            ...     for row in scanner:
            ...         print(row)
        """
        # Use cluster-level scanner which will locate regions and iterate across them
        return self.get_scanner(scan)

    def get_scanner(self, scan: Scan) -> ResultScanner:
        """Get a ResultScanner for the table.

        This is an internal method that creates a cluster-level scanner
        capable of iterating across multiple regions.

        Args:
            scan: Scan operation

        Returns:
            ResultScanner for iterating through results
        """
        # Ensure a cluster connection is available so scanners can locate regions spanning the cluster
        if self.cluster_conn is None:
            self.cluster_conn = ClusterConnection(self.conf)
        return ResultScanner(scan, TableName.value_of(self.ns, self.tb), self.cluster_conn)

    def scan_page(self, scan: Scan, page_size: int) -> tuple[list['Row'], bytes | None]:
        """Stateless pagination helper: open a scanner, fetch up to page_size rows, close and return (rows, resume_key)

        Resume key is the last returned row's key (client should use start_row=resume_key and include_start_row=False to continue).
        """
        scanner = self.get_scanner(scan)
        try:
            rows = scanner.next_batch(page_size)
            resume = None
            if rows:
                resume = rows[-1].rowkey
            return rows, resume
        finally:
            try:
                scanner.close()
            except Exception:
                pass

    def batch(self, batch_size: int = 1000) -> Batch:
        """
        Create a Batch context manager for grouping operations.

        Example:
            with table.batch() as b:
                b.put(b'row1', {b'cf:col1': b'value1'})
                b.put(b'row2', {b'cf:col1': b'value2'})
                b.delete(b'row3')
            # All operations are executed when exiting the context

        Args:
            batch_size: Maximum number of operations to buffer before auto-flush

        Returns:
            Batch context manager
        """
        return Batch(self, batch_size)

    def batch_get(self, batch_get: BatchGet) -> dict[bytes, 'Row | None']:
        """
        Retrieve multiple rows efficiently.

        Example:
            bg = BatchGet([b'row1', b'row2', b'row3'])
            bg.add_column(b'cf', b'col1')

            results = table.batch_get(bg)
            for rowkey, row in results.items():
                print(rowkey, row)

        Args:
            batch_get: BatchGet operation specifying rows and columns to retrieve

        Returns:
            Dictionary mapping rowkey to Row object (or None if not found)
        """
        results: dict[bytes, 'Row | None'] = {}

        for rowkey in batch_get.rowkeys:
            get = Get(rowkey)

            # Copy settings from BatchGet
            for family, qualifiers in batch_get.family_columns.items():
                if not qualifiers:
                    # Add entire family
                    get.add_family(family)
                else:
                    # Add specific qualifiers
                    for qualifier in qualifiers:
                        get.add_column(family, qualifier)

            get.read_versions(batch_get.max_versions)
            get.set_time_range(batch_get.time_ranges[0], batch_get.time_ranges[1])
            get.set_check_existence_only(batch_get.check_existence_only)

            row = self.get(get)
            results[rowkey] = row

        return results

    def batch_put(self, batch_put: BatchPut) -> list[bool]:
        """
        Insert multiple rows efficiently.

        Example:
            bp = BatchPut()
            bp.add_put(Put(b'row1').add_column(b'cf', b'col1', b'value1'))
            bp.add_put(Put(b'row2').add_column(b'cf', b'col1', b'value2'))

            results = table.batch_put(bp)

        Args:
            batch_put: BatchPut operation containing puts to execute

        Returns:
            List of boolean values indicating success of each put
        """
        results: list[bool] = []

        for put in batch_put.puts:
            result = self.put(put)
            results.append(result)

        return results

    def check_and_put(self, check_and_put: CheckAndPut) -> bool:
        """
        Perform a conditional put operation (server-side atomic).

        This operation atomically checks if a column has a specific value
        before performing a put. If the check passes, the put is performed.

        Example:
            cap = CheckAndPut(b'row1')
            cap.set_check(b'cf', b'lock', b'')
            cap.set_put(Put(b'row1').add_column(b'cf', b'data', b'value'))

            success = table.check_and_put(cap)
            if success:
                print("Put succeeded")
            else:
                print("Check failed")

        Args:
            check_and_put: CheckAndPut operation

        Returns:
            True if check passed and put was performed, False otherwise
        """
        if not check_and_put.validate():
            raise ValueError("Invalid CheckAndPut operation")

        region: Region = self.locate_target_region(check_and_put.rowkey)
        conn = self.get_rs_connection(region)

        # Build column values for the put
        column_values = []
        for family, cells in check_and_put.put_operation.family_cells.items():
            qualifiers = [(cell.qualifier, cell.value) for cell in cells]
            column_values.append((family, qualifiers))

        # Map compare operator to CompareType
        compare_type = CompareType.EQUAL
        if check_and_put.compare_op == "NOT_EQUAL":
            compare_type = CompareType.NOT_EQUAL
        elif check_and_put.compare_op == "LESS":
            compare_type = CompareType.LESS
        elif check_and_put.compare_op == "LESS_OR_EQUAL":
            compare_type = CompareType.LESS_OR_EQUAL
        elif check_and_put.compare_op == "GREATER":
            compare_type = CompareType.GREATER
        elif check_and_put.compare_op == "GREATER_OR_EQUAL":
            compare_type = CompareType.GREATER_OR_EQUAL

        success, _ = conn.check_and_mutate(
            region.region_encoded,
            check_and_put.rowkey,
            check_and_put.check_family,
            check_and_put.check_qualifier,
            check_and_put.check_value,
            compare_type,
            MutationProto.MutationType.PUT,
            column_values
        )
        return success

    def check_and_delete(
        self,
        rowkey: bytes,
        check_family: bytes,
        check_qualifier: bytes,
        check_value: bytes | None,
        delete: Delete,
        compare_op: str = "EQUAL"
    ) -> bool:
        """
        Perform a conditional delete operation (server-side atomic).

        This operation atomically checks if a column has a specific value
        before performing a delete. If the check passes, the delete is performed.

        Example:
            delete = Delete(b'row1').add_column(b'cf', b'col')
            success = table.check_and_delete(
                b'row1', b'cf', b'lock', b'', delete
            )
            if success:
                print("Delete succeeded")

        Args:
            rowkey: Row key
            check_family: Column family to check
            check_qualifier: Column qualifier to check
            check_value: Expected value (None checks for non-existence)
            delete: Delete operation to perform if check passes
            compare_op: Comparison operator ('EQUAL', 'NOT_EQUAL', etc.)

        Returns:
            True if check passed and delete was performed, False otherwise
        """
        region: Region = self.locate_target_region(rowkey)
        conn = self.get_rs_connection(region)

        # Build column values for the delete
        column_values = []
        for family, cells in delete.family_cells.items():
            qualifiers = [(cell.qualifier, b'') for cell in cells]
            column_values.append((family, qualifiers))

        # Map compare operator to CompareType
        compare_type = CompareType.EQUAL
        if compare_op == "NOT_EQUAL":
            compare_type = CompareType.NOT_EQUAL
        elif compare_op == "LESS":
            compare_type = CompareType.LESS
        elif compare_op == "LESS_OR_EQUAL":
            compare_type = CompareType.LESS_OR_EQUAL
        elif compare_op == "GREATER":
            compare_type = CompareType.GREATER
        elif compare_op == "GREATER_OR_EQUAL":
            compare_type = CompareType.GREATER_OR_EQUAL

        success, _ = conn.check_and_mutate(
            region.region_encoded,
            rowkey,
            check_family,
            check_qualifier,
            check_value,
            compare_type,
            MutationProto.MutationType.DELETE,
            column_values
        )
        return success

    def increment(self, increment: Increment) -> int:
        """
        Atomically increment a counter value (server-side atomic).

        This operation atomically increments a column value by a specified amount.
        If the column does not exist, it is created with the increment value.

        Example:
            inc = Increment(b'row1')
            inc.add_column(b'cf', b'counter', 1)

            new_value = table.increment(inc)
            print(f"New counter value: {new_value}")

        Args:
            increment: Increment operation

        Returns:
            New counter value after increment (for single column),
            or the last incremented value (for multiple columns)
        """
        region: Region = self.locate_target_region(increment.rowkey)
        conn = self.get_rs_connection(region)

        # Build column values for the increment
        column_values = []
        for family, cells in increment.family_cells.items():
            qualifiers = []
            for cell in cells:
                try:
                    amount = int(cell.value.decode('utf-8'))
                except (ValueError, TypeError, UnicodeDecodeError):
                    amount = 1
                qualifiers.append((cell.qualifier, amount))
            column_values.append((family, qualifiers))

        row = conn.increment(
            region.region_encoded,
            increment.rowkey,
            column_values,
            return_results=increment.return_results
        )

        if row is None:
            return 0

        # Extract the first (or last) counter value
        for family, cells in increment.family_cells.items():
            for cell in cells:
                value = row.get(family, cell.qualifier)
                if value is not None:
                    # Decode 8-byte big-endian integer
                    return int.from_bytes(value, byteorder='big', signed=True)
        return 0

    def append(self, append: Append) -> Row | None:
        """
        Atomically append to column values (server-side atomic).

        This operation atomically appends data to the end of existing column values.
        If the column does not exist, the append value becomes the initial value.

        Example:
            append = Append(b'row1')
            append.add_column(b'cf', b'suffix_col', b'_suffix')
            append.add_column(b'cf', b'tags', b',new_tag')

            result = table.append(append)
            new_value = result.get(b'cf', b'suffix_col')

        Args:
            append: Append operation

        Returns:
            Row containing the new values after append, or None if return_results is False
        """
        region: Region = self.locate_target_region(append.rowkey)
        conn = self.get_rs_connection(region)

        # Build column values for the append
        column_values = []
        for family, cells in append.family_cells.items():
            qualifiers = [(cell.qualifier, cell.value) for cell in cells]
            column_values.append((family, qualifiers))

        return conn.append(
            region.region_encoded,
            append.rowkey,
            column_values,
            return_results=append.return_results
        )

    def mutate_row(self, row_mutations: 'RowMutations') -> bool:
        """Execute multiple mutations atomically on a single row.

        This operation combines multiple mutations (Put, Delete, Increment, Append)
        into a single atomic operation. All mutations are applied together or none are.

        Example:
            >>> from hbasedriver.operations import RowMutations, Put, Delete
            >>>
            >>> rm = RowMutations(b'row1')
            >>> rm.add(Put(b'row1').add_column(b'cf', b'col1', b'value1'))
            >>> rm.add(Delete(b'row1').add_column(b'cf', b'col2'))
            >>>
            >>> success = table.mutate_row(rm)
            >>> if success:
            ...     print("All mutations applied atomically")

        Args:
            row_mutations: RowMutations object containing the mutations

        Returns:
            True if all mutations were applied successfully

        Raises:
            ValueError: If no mutations are provided
        """
        if not row_mutations:
            raise ValueError("RowMutations must contain at least one mutation")

        region: Region = self.locate_target_region(row_mutations.rowkey)
        conn = self.get_rs_connection(region)

        # Build mutation list for the server
        mutations = []
        for mutation in row_mutations.get_mutations():
            column_values = []

            if isinstance(mutation, Put):
                mutation_type = MutationProto.MutationType.PUT
                for family, cells in mutation.family_cells.items():
                    qualifiers = [(cell.qualifier, cell.value) for cell in cells]
                    column_values.append((family, qualifiers))
            elif isinstance(mutation, Delete):
                mutation_type = MutationProto.MutationType.DELETE
                for family, cells in mutation.family_cells.items():
                    qualifiers = [(cell.qualifier, b'') for cell in cells]
                    column_values.append((family, qualifiers))
            elif isinstance(mutation, Increment):
                mutation_type = MutationProto.MutationType.INCREMENT
                for family, cells in mutation.family_cells.items():
                    qualifiers = []
                    for cell in cells:
                        try:
                            amount = int(cell.value.decode('utf-8'))
                        except (ValueError, TypeError, UnicodeDecodeError):
                            amount = 1
                        qualifiers.append((cell.qualifier, amount))
                    column_values.append((family, qualifiers))
            elif isinstance(mutation, Append):
                mutation_type = MutationProto.MutationType.APPEND
                for family, cells in mutation.family_cells.items():
                    qualifiers = [(cell.qualifier, cell.value) for cell in cells]
                    column_values.append((family, qualifiers))
            else:
                raise ValueError(f"Unsupported mutation type: {type(mutation).__name__}")

            mutations.append((mutation_type, column_values))

        return conn.mutate_row(
            region.region_encoded,
            row_mutations.rowkey,
            mutations
        )

    def get_rs_connection(self, region: Region) -> RsConnection:
        """Get a connection to the region server from the connection pool.

        Args:
            region: The target region

        Returns:
            An RsConnection to the region server
        """
        host = region.host.decode('utf-8') if isinstance(region.host, bytes) else region.host
        port = int(region.port.decode('utf-8')) if isinstance(region.port, bytes) else int(region.port)

        # Try the connection pool first
        if self._conn_pool:
            return self._conn_pool.get_connection(host, port)

        # Fallback to old cache-based approach for backward compatibility
        conn = self.rs_conns.get((region.host, region.port))
        if not conn:
            conn: RsConnection = RsConnection().connect(region.host, region.port)
            self.rs_conns[(region.host, region.port)] = conn
        return conn

    def locate_target_region(self, rowkey: bytes) -> Region:
        # check cached regions first, return if we already touched that region.
        # Use TTL-based validation to reduce meta lookups for frequently accessed regions.
        current_time = time.time()

        for region_id, (region, cached_time) in self.regions.items():
            if region.key_in_region(rowkey):
                # If cached within TTL, skip validation (fast path)
                if current_time - cached_time < self.REGION_CACHE_TTL:
                    return region

                # Otherwise validate against meta to avoid using stale encoded region names
                # that can occur after delete+create (e.g., truncate fallback).
                try:
                    if self._conn_pool:
                        meta_conn = self._conn_pool.get_connection(self.meta_rs_host, self.meta_rs_port)
                    else:
                        meta_conn = MetaRsConnection().connect(self.meta_rs_host, self.meta_rs_port)
                    fresh = meta_conn.locate_region(self.ns, self.tb, rowkey)
                    # If the encoded name matches, cached entry is still valid
                    if fresh.get_region_name() == region.get_region_name():
                        # Update cache timestamp for the validated region
                        self.regions[region_id] = (region, current_time)
                        return region
                    # Otherwise replace stale cache with fresh region info
                    self.regions[fresh.region_info.region_id] = (fresh, current_time)
                    try:
                        del self.regions[region_id]
                    except Exception:
                        pass
                    return fresh
                except Exception:
                    # If meta lookup fails, fall back to cached region to avoid breaking callers.
                    return region

        # Meta server connections are special (MetaRsConnection) and have additional
        # methods like locate_region() that regular RsConnection doesn't have.
        # Therefore, we don't use the connection pool for meta server connections.
        conn = MetaRsConnection().connect(self.meta_rs_host, self.meta_rs_port)
        region = conn.locate_region(self.ns, self.tb, rowkey)
        self.regions[region.region_info.region_id] = (region, current_time)
        return region

    def close(self):
        """Clean up resources including closing all pooled connections.

        This should be called when the table is no longer needed to properly
        release resources back to the connection pool or close connections.
        """
        # Close all connections in the pool
        if self._conn_pool:
            self._conn_pool.close_all()

        # Close cluster connection if present
        if self.cluster_conn:
            self.cluster_conn.close()
            self.cluster_conn = None

        # Clear cached connections (old style, for backward compatibility)
        self.rs_conns.clear()

    def invalidate_cache(self) -> None:
        """Invalidate the region cache.

        This method clears the cached region information, forcing the next
        operation to fetch fresh region data from the meta server.

        This should be called after operations that modify the table's region
        layout, such as truncating the table, to ensure subsequent
        operations use the correct region information.

        Example:
            >>> table = client.get_table(b"default", b"mytable")
            >>> # Use table...
            >>> admin.truncate_table(tn, preserve_splits=False)
            >>> # Invalidate cache to use new region information
            >>> table.invalidate_cache()
            >>> # Now operations will use fresh region data
            >>> table.get(Get(b"row1"))
        """
        self.regions.clear()

    def get_pool_stats(self) -> dict:
        """Get connection pool statistics.

        Returns:
            Dictionary containing pool statistics including:
                - pool_size: Maximum pool size
                - active: Number of active connections
                - idle: Number of idle connections
                - created: Total number of connections created
                - borrowed: Total number of connection borrows
        """
        if self._conn_pool:
            return self._conn_pool.get_stats()
        return {}

    def __enter__(self):
        """Enter the context manager.

        Returns:
            The Table instance
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
