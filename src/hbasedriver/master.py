import time

from hbasedriver.protobuf_py.HBase_pb2 import ColumnFamilySchema, TableName
from hbasedriver.protobuf_py.Master_pb2 import CreateTableRequest, DeleteTableRequest, DisableTableRequest, \
    GetTableDescriptorsRequest, GetTableDescriptorsResponse
from hbasedriver.Connection import Connection


class MasterConnection(Connection):
    def clone(self):
        return MasterConnection.connect(self.host, self.port, self.timeout)

    service_name = "MasterService"

    def __init__(self):
        super().__init__("MasterService")

    def create_table(self, namespace: bytes, table: bytes, columns: list[ColumnFamilySchema], split_keys=None):
        """
        Create a hbase table, raise RemoteException if failed or table exists.
        :param namespace:
        :param table:
        :param columns:
        :param split_keys:
        :return: None
        """
        if split_keys is None:
            split_keys = []
        rq = CreateTableRequest()

        rq.split_keys.extend(split_keys)
        rq.table_schema.table_name.namespace = namespace
        rq.table_schema.table_name.qualifier = table
        # add all column definitions
        for c in columns:
            rq.table_schema.column_families.append(c)

        self.send_request(rq, "CreateTable")

    def enable_table(self, ns: bytes, tb: bytes):
        rq = DisableTableRequest()
        if ns is None or len(ns) == 0:
            ns = b"default"
        rq.table_name.namespace = ns
        rq.table_name.qualifier = tb
        self.send_request(rq, "EnableTable")
        # sleep some time for hbase to react.
        time.sleep(1)

        # todo: check table enabled.

    def disable_table(self, namespace: bytes, table: bytes):
        rq = DisableTableRequest()
        if namespace is None or len(namespace) == 0:
            namespace = b'default'
        rq.table_name.namespace = namespace
        rq.table_name.qualifier = table
        self.send_request(rq, "DisableTable")
        # todo: check table disabled.

    def describe_table(self, namespace: bytes, table: bytes) -> GetTableDescriptorsResponse:
        if namespace is None or len(namespace) == 0:
            namespace = b"default"
        rq = GetTableDescriptorsRequest()
        rq.table_names.append(TableName(namespace=namespace, qualifier=table))
        return self.send_request(rq, "GetTableDescriptors")

    def list_table_descriptors(self, pattern: str, include_sys_table: bool) -> GetTableDescriptorsResponse:
        rq = GetTableDescriptorsRequest()
        rq.regex = pattern
        rq.include_sys_tables = include_sys_table
        return self.send_request(rq, "GetTableDescriptors")

    def delete_table(self, namespace: bytes, table: bytes):
        rq = DeleteTableRequest()
        if namespace is None or len(namespace) == 0:
            namespace = b'default'

        rq.table_name.namespace = namespace
        rq.table_name.qualifier = table

        self.send_request(rq, "DeleteTable")
        # todo : check regions offline.
