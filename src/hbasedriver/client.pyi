"""
Type stubs for hbase-driver.client module.

This file provides type hints for IDE autocomplete without importing
the actual implementations at runtime.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hbasedriver.operations.put import Put
    from hbasedriver.operations.get import Get
    from hbasedriver.operations.delete import Delete
    from hbasedriver.operations.scan import Scan
    from hbasedriver.operations.batch import BatchGet, BatchPut
    from hbasedriver.operations.increment import Increment, CheckAndPut

    from hbasedriver.common.table_name import TableName

    from hbasedriver.model import Row, Cell, CellType

    class Client:
        """Main client class for connecting to and interacting with HBase.

        The Client class acts like HBase Java Connection interface.
        It provides access to Admin (for DDL operations), Table (for DML
        operations), and region metadata.

        Example:
            >>> config = {"hbase.zookeeper.quorum": "localhost:2181"}
            >>> client = Client(config)
            >>> admin = client.get_admin()
            >>> table = client.get_table("default", "mytable")
            >>>
        """

        conf: dict
        zk_quorum: list[str]
        master_host: str
        master_port: int
        meta_host: str
        meta_port: int
        cluster_connection: object
        master_conn: object
        meta_conn: object

        def __init__(self, conf: dict) -> None: ...

        def get_admin(self) -> Admin: ...

        def get_table(self, ns: bytes | None, tb: bytes) -> Table: ...

        def describe_table(self, ns: bytes, tb: bytes) -> object: ...

        def check_regions_online(self, ns: bytes, tb: bytes, split_keys: list[bytes]) -> None: ...

        def close(self) -> None: ...

        def __enter__(self) -> Client: ...

        def __exit__(self, exc_type, exc_val, exc_tb) -> bool: ...

    class Admin:
        """Admin class for performing DDL operations on HBase.

        Provides table management operations like create, disable, enable,
        delete, and truncate tables.

        Example:
            >>> admin = client.get_admin()
            >>> admin.create_table(tn, [cf1, cf2])
            >>> admin.disable_table(tn)
            >>> admin.enable_table(tn)
            >>> admin.delete_table(tn)
            >>> admin.truncate_table(tn, preserve_splits=False)
            >>>
        """

        client: object

        def create_table(self, ns: bytes, tb: bytes, column_families: list, split_keys: list[bytes] = None) -> None: ...

        def disable_table(self, table_name: TableName) -> None: ...

        def enable_table(self, table_name: TableName) -> None: ...

        def delete_table(self, table_name: TableName) -> None: ...

        def table_exists(self, table_name: TableName) -> bool: ...

        def truncate_table(self, table_name: TableName, preserve_splits: bool = False, timeout: int = 60) -> None: ...

        def is_table_enabled(self, table_name: TableName) -> bool: ...

        def get_table_state(self, ns: bytes, tb: bytes) -> object: ...

        def list_tables(self) -> list: ...

        def list_namespaces(self) -> list: ...

        def create_namespace(self, namespace_name: bytes) -> None: ...

        def delete_namespace(self, namespace_name: bytes) -> None: ...

        def namespace_exists(self, namespace_name: bytes) -> bool: ...

        def close(self) -> None: ...

    class Table:
        """Table class for performing data operations on a specific HBase table.

        This class provides methods for Put, Get, Delete, Scan, Batch,
        CheckAndPut, and Increment operations on a specific table. Each Table
        instance maintains its own connection pool and region cache.

        Example:
            >>> table = client.get_table("default", "mytable")
            >>> table.put(Put(b"row1").add_column(b"cf", b"col", b"value"))
            >>> row = table.get(Get(b"row1"))
            >>> with table.scan(Scan()) as scanner:
            ...     for row in scanner:
            ...         print(row)
            >>>
        """

        ns: bytes
        tb: bytes
        conf: dict
        meta_rs_host: str
        meta_rs_port: int
        regions: dict[int, tuple[object, float]]
        _conn_pool: object
        rs_conns: dict[tuple[bytes, int], object]
        cluster_conn: object | None

        REGION_CACHE_TTL: int

        def __init__(self, conf: dict, ns: bytes | str, tb: bytes | str) -> None: ...

        def put(self, put: Put) -> bool: ...

        def get(self, get: Get) -> Row | None: ...

        def delete(self, delete: Delete) -> bool: ...

        def scan(self, scan: Scan) -> ResultScanner: ...

        def batch_get(self, batch_get: BatchGet) -> list[Row]: ...

        def batch_put(self, batch_put: BatchPut) -> list[object]: ...

        def batch(self, batch_size: int = 1000) -> object: ...

        def increment(self, increment: Increment) -> int | None: ...

        def check_and_put(self, cap: CheckAndPut) -> bool: ...

        def scan_page(self, scan: Scan, page_size: int = 1000) -> tuple[list[Row], bytes | None]: ...

        def locate_target_region(self, rowkey: bytes) -> object: ...

        def get_rs_connection(self, region: object) -> object: ...

        def invalidate_cache(self) -> None: ...

        def close(self) -> None: ...

        def get_pool_stats(self) -> dict: ...

        def __enter__(self) -> Table: ...

        def __exit__(self, exc_type, exc_val, exc_tb) -> bool: ...

    class ResultScanner:
        """ResultScanner for iterating over scan results.

        Provides iterator interface for scan results with automatic
        resource cleanup on exit.

        Example:
            >>> with table.scan(Scan()) as scanner:
            ...     for row in scanner:
            ...         print(row)
            >>>
        """

        def __init__(self, table: Table, scan: Scan, client: object) -> None: ...

        def __iter__(self) -> ResultScanner: ...

        def __next__(self) -> Row: ...

        def close(self) -> None: ...

        def __enter__(self) -> ResultScanner: ...

        def __exit__(self, exc_type, exc_val, exc_tb) -> bool: ...

else:
    # Runtime type stubs - minimal imports to avoid import errors
    Client = object  # type: ignore
    Admin = object  # type: ignore
    Table = object  # type: ignore
    ResultScanner = object  # type: ignore
    Row = object  # type: ignore
    Cell = object  # type: ignore
    CellType = object  # type: ignore
    TableName = object  # type: ignore
    Put = object  # type: ignore
    Get = object  # type: ignore
    Delete = object  # type: ignore
    Scan = object  # type: ignore
    BatchGet = object  # type: ignore
    BatchPut = object  # type: ignore
    Increment = object  # type: ignore
    CheckAndPut = object  # type: ignore
