import time

from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema, TableName as PBTableName
from hbasedriver.protobuf_py.Master_pb2 import (
    CreateTableRequest,
    DeleteTableRequest,
    DisableTableRequest,
    EnableTableRequest,
    GetTableDescriptorsRequest,
    GetTableDescriptorsResponse
)
from hbasedriver.Connection import Connection


class MasterConnection(Connection):
    def __init__(self):
        super().__init__("MasterService")

    def clone(self):
        return MasterConnection().connect(self.host, self.port, self.timeout)

    def create_table(self, namespace: bytes, table: bytes, columns: list[ColumnFamilySchema], split_keys=None):
        rq = CreateTableRequest()
        rq.split_keys.extend(split_keys or [])
        rq.table_schema.table_name.namespace = namespace or b'default'
        rq.table_schema.table_name.qualifier = table
        for c in columns:
            rq.table_schema.column_families.append(c)
        self.send_request(rq, "CreateTable")

    def enable_table(self, namespace: bytes, table: bytes):
        rq = EnableTableRequest()
        rq.table_name.namespace = namespace or b"default"
        rq.table_name.qualifier = table
        self.send_request(rq, "EnableTable")
        time.sleep(1)

    def disable_table(self, namespace: bytes, table: bytes):
        rq = DisableTableRequest()
        rq.table_name.namespace = namespace or b'default'
        rq.table_name.qualifier = table
        self.send_request(rq, "DisableTable")

    def delete_table(self, namespace: bytes, table: bytes):
        rq = DeleteTableRequest()
        rq.table_name.namespace = namespace or b'default'
        rq.table_name.qualifier = table
        self.send_request(rq, "DeleteTable")

    def describe_table(self, namespace: bytes, table: bytes) -> GetTableDescriptorsResponse:
        rq = GetTableDescriptorsRequest()
        pb_name = PBTableName(namespace=namespace or b'default', qualifier=table)
        rq.table_names.append(pb_name)
        return self.send_request(rq, "GetTableDescriptors")

    def list_table_descriptors(self, pattern: str = ".*",
                               include_sys_table: bool = False) -> GetTableDescriptorsResponse:
        rq = GetTableDescriptorsRequest()
        rq.regex = pattern
        rq.include_sys_tables = include_sys_table
        return self.send_request(rq, "GetTableDescriptors")
