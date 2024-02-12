import random
import socket
from io import StringIO
from struct import pack, unpack

from google.protobuf.pyext import _message

from Client_pb2 import GetRequest, Column
from HBase_pb2 import ColumnFamilySchema
from RPC_pb2 import ConnectionHeader, RequestHeader, ResponseHeader
from protobuf_py.Master_pb2 import CreateTableRequest, DeleteTableRequest, DisableTableRequest
from src.Connection import Connection
from src.response import response_types
from util.varint import to_varint, decoder

from google.protobuf import message


class MasterConnection(Connection):
    service_name = "MasterService"

    def __init__(self):
        super().__init__("MasterService")

    def create_table(self, namespace, table, columns, split_keys=None):
        if split_keys is None:
            split_keys = []
        rq = CreateTableRequest()

        rq.split_keys.extend(split_keys)
        rq.table_schema.table_name.namespace = namespace.encode("utf-8")
        rq.table_schema.table_name.qualifier = table.encode("utf-8")
        # add all column definitions
        for c in columns:
            rq.table_schema.column_families.append(ColumnFamilySchema(name=c.encode("utf-8")))

        self.send_request(rq, "CreateTable")
        # todo: check regions online.

    def enable_table(self, ns, tb):
        rq = DisableTableRequest()
        rq.table_name.namespace = ns.encode("utf-8")
        rq.table_name.qualifier = tb.encode("utf-8")
        self.send_request(rq, "EnableTable")
        # todo: check table enabled.

    def disable_table(self, namespace, table):
        rq = DisableTableRequest()
        rq.table_name.namespace = namespace.encode("utf-8")
        rq.table_name.qualifier = table.encode("utf-8")
        self.send_request(rq, "DisableTable")
        # todo: check table disabled.

    def delete_table(self, namespace, table):
        rq = DeleteTableRequest()

        rq.table_name.namespace = namespace.encode("utf-8")
        rq.table_name.qualifier = table.encode("utf-8")

        self.send_request(rq, "DeleteTable")
        # todo : check regions offline.
