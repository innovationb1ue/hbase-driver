# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: MapReduce.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import HBase_pb2 as HBase__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0fMapReduce.proto\x12\x08hbase.pb\x1a\x0bHBase.proto\"7\n\x0bScanMetrics\x12(\n\x07metrics\x18\x01 \x03(\x0b\x32\x17.hbase.pb.NameInt64Pair\"y\n\x18TableSnapshotRegionSplit\x12\x11\n\tlocations\x18\x02 \x03(\t\x12$\n\x05table\x18\x03 \x01(\x0b\x32\x15.hbase.pb.TableSchema\x12$\n\x06region\x18\x04 \x01(\x0b\x32\x14.hbase.pb.RegionInfoBI\n1org.apache.hadoop.hbase.shaded.protobuf.generatedB\x0fMapReduceProtosH\x01\xa0\x01\x01')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'MapReduce_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  _globals['DESCRIPTOR']._options = None
  _globals['DESCRIPTOR']._serialized_options = b'\n1org.apache.hadoop.hbase.shaded.protobuf.generatedB\017MapReduceProtosH\001\240\001\001'
  _globals['_SCANMETRICS']._serialized_start=42
  _globals['_SCANMETRICS']._serialized_end=97
  _globals['_TABLESNAPSHOTREGIONSPLIT']._serialized_start=99
  _globals['_TABLESNAPSHOTREGIONSPLIT']._serialized_end=220
# @@protoc_insertion_point(module_scope)