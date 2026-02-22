from hbasedriver.meta_server import MetaRsConnection
from hbasedriver.connection.connection_pool import ConnectionPool
from hbasedriver.region_locations import RegionLocations
from hbasedriver.table_name import TableName
from hbasedriver.zk_connection_registry import ZKConnectionRegistry

from abc import ABC, abstractmethod


class ClusterConnection(ABC):
    def __init__(self, conf: dict):
        # Connection pool for meta connections
        self._conn_pool = ConnectionPool(
            pool_size=conf.get('hbase.meta.connection.pool.size', 5),
            idle_timeout=conf.get('hbase.meta.connection.idle.timeout', 300),
            connection_factory=lambda host, port: MetaRsConnection().connect(host, port)
        )
        # Deprecated: kept for backward compatibility, using connection pool now
        self.conn_map = {}
        self.registry = ZKConnectionRegistry(conf)
        self.meta_locations: RegionLocations | None = None
        self.regions = {}

    # Find the location of the region of tableName that row lives in.
    def locate_region(self, table_name: TableName, row, use_cache=True) -> RegionLocations:
        if table_name is None or len(table_name.tb) == 0:
            raise Exception("table name cannot be null or zero length")
        if table_name == TableName.META_TABLE_NAME:
            if use_cache and self.meta_locations is not None:
                return self.meta_locations
            self.meta_locations = self.registry.get_meta_region_locations()
            return self.meta_locations
        else:
            return self.locate_region_in_meta(table_name, row)

    def locate_region_in_meta(self, table_name: TableName, rowkey: bytes) -> 'Region':
        # check cached regions first, return if we already touched that region.
        for region in self.regions.values():
            if region.key_in_region(rowkey):
                return region

        if not self.meta_locations:
            self.meta_locations = self.registry.get_meta_region_locations()

        if len(self.meta_locations.locations) == 0:
            raise Exception("meta region not open?")

        # Use connection pool for meta connection
        host = self.meta_locations.locations[0].server_name.host
        if isinstance(host, bytes):
            host = host.decode('utf-8')
        port = self.meta_locations.locations[0].server_name.port
        if isinstance(port, bytes):
            port = int(port.decode('utf-8'))
        conn = self._conn_pool.get_connection(host, port)
        region = conn.locate_region(table_name.ns, table_name.tb, rowkey)
        if region is None:
            raise Exception("can not locate region ")
        self.regions[region.region_info.region_id] = region
        return region

    def get_meta_connection(self, host: str, port: int):
        """Get a meta connection from the pool.

        Args:
            host: Meta region server host
            port: Meta region server port

        Returns:
            A meta connection from the pool
        """
        return self._conn_pool.get_connection(host, port)

    def return_meta_connection(self, connection):
        """Return a meta connection to the pool.

        Args:
            connection: The meta connection to return
        """
        self._conn_pool.return_connection(connection)

    def close(self):
        """Clean up resources including closing all pooled connections."""
        if self._conn_pool:
            self._conn_pool.close_all()

        self.regions.clear()
        self.meta_locations = None
