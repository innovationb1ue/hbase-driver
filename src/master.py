import random
import socket
from io import StringIO
from struct import pack, unpack

from google.protobuf.pyext import _message

from Client_pb2 import GetRequest, Column
from HBase_pb2 import ColumnFamilySchema
from RPC_pb2 import ConnectionHeader, RequestHeader, ResponseHeader
from protobuf_py.Master_pb2 import CreateTableRequest
from src.Connection import Connection
from src.response import response_types
from util.varint import to_varint, decoder

from google.protobuf import message


class MasterConnection(Connection):
    service_name = "MasterService"

    def __init__(self):
        super().__init__("MasterService")

    def create_table(self, namespace, table, columns):
        rq = CreateTableRequest()
        rq.table_schema.table_name.namespace = namespace.encode("utf-8")
        rq.table_schema.table_name.qualifier = table.encode("utf-8")
        # add all column definitions
        for c in columns:
            rq.table_schema.column_families.append(ColumnFamilySchema(name=c.encode("utf-8")))
        rpc_serialized = rq.SerializeToString()

        # get RPC header
        serialized_header: bytes = self._get_call_header_bytes("CreateTable")

        # length of data
        rpc_length_bytes: bytes = to_varint(len(rpc_serialized)).encode("utf-8")

        # 4byte total message size + header_size + header size
        total_size = 4 + 1 + len(serialized_header) + len(rpc_length_bytes) + len(rpc_serialized)
        print("total size = ", total_size)

        # Total length doesn't include the initial 4 bytes (for the total_length uint32)
        # size(4bytes) + header size(1byte)
        to_send = pack(">IB", total_size - 4, len(serialized_header))
        to_send += serialized_header + rpc_length_bytes + rpc_serialized

        self.conn.send(to_send)

        # todo: check regions online.
