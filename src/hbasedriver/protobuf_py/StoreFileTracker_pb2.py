# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: StoreFileTracker.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x16StoreFileTracker.proto\x12\x08hbase.pb\",\n\x0eStoreFileEntry\x12\x0c\n\x04name\x18\x01 \x02(\t\x12\x0c\n\x04size\x18\x02 \x02(\x04\"P\n\rStoreFileList\x12\x11\n\ttimestamp\x18\x01 \x02(\x04\x12,\n\nstore_file\x18\x02 \x03(\x0b\x32\x18.hbase.pb.StoreFileEntryBS\n1org.apache.hadoop.hbase.shaded.protobuf.generatedB\x16StoreFileTrackerProtosH\x01\x88\x01\x01\xa0\x01\x01')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'StoreFileTracker_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  _globals['DESCRIPTOR']._options = None
  _globals['DESCRIPTOR']._serialized_options = b'\n1org.apache.hadoop.hbase.shaded.protobuf.generatedB\026StoreFileTrackerProtosH\001\210\001\001\240\001\001'
  _globals['_STOREFILEENTRY']._serialized_start=36
  _globals['_STOREFILEENTRY']._serialized_end=80
  _globals['_STOREFILELIST']._serialized_start=82
  _globals['_STOREFILELIST']._serialized_end=162
# @@protoc_insertion_point(module_scope)
