# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: RSGroup.proto
# Protobuf Python Version: 4.25.2
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import Table_pb2 as Table__pb2
import HBase_pb2 as HBase__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\rRSGroup.proto\x12\x02pb\x1a\x0bTable.proto\x1a\x0bHBase.proto\"\x86\x01\n\x0bRSGroupInfo\x12\x0c\n\x04name\x18\x01 \x02(\t\x12\x1f\n\x07servers\x18\x04 \x03(\x0b\x32\x0e.pb.ServerName\x12\x1d\n\x06tables\x18\x03 \x03(\x0b\x32\r.pb.TableName\x12)\n\rconfiguration\x18\x05 \x03(\x0b\x32\x12.pb.NameStringPairBC\n*org.apache.hadoop.hbase.protobuf.generatedB\rRSGroupProtosH\x01\x88\x01\x01\xa0\x01\x01')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'RSGroup_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  _globals['DESCRIPTOR']._options = None
  _globals['DESCRIPTOR']._serialized_options = b'\n*org.apache.hadoop.hbase.protobuf.generatedB\rRSGroupProtosH\001\210\001\001\240\001\001'
  _globals['_RSGROUPINFO']._serialized_start=48
  _globals['_RSGROUPINFO']._serialized_end=182
# @@protoc_insertion_point(module_scope)