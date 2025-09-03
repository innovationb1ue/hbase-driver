from hbasedriver.meta_server import MetaRsConnection
from hbasedriver.region_locations import RegionLocations
from hbasedriver.table_name import TableName
from hbasedriver.zk_connection_registry import ZKConnectionRegistry

from abc import ABC, abstractmethod


class ClusterConnection(ABC):
    def __init__(self, conf: dict):
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

    # todo: make this return HRegionLocations.
    def locate_region_in_meta(self, table_name: TableName, rowkey: bytes):
        # check cached regions first, return if we already touched that region.
        for region in self.regions.values():
            if region.key_in_region(rowkey):
                return region

        if not self.meta_locations:
            self.meta_locations = self.registry.get_meta_region_locations()

        if len(self.meta_locations.locations) == 0:
            raise Exception("meta region not open?")

        conn = MetaRsConnection().connect(self.meta_locations.locations[0].server_name.host,
                                          self.meta_locations.locations[0].server_name.port)
        region = conn.locate_region(table_name.ns, table_name.tb, rowkey)
        if region is None:
            raise Exception("can not locate region ")
        self.regions[region.region_info.region_id] = region
        return region
