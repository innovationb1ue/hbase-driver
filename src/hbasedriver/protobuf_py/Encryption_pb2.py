# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: Encryption.proto
# Protobuf Python Version: 4.25.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x10\x45ncryption.proto\x12\x08hbase.pb\"t\n\nWrappedKey\x12\x11\n\talgorithm\x18\x01 \x02(\t\x12\x0e\n\x06length\x18\x02 \x02(\r\x12\x0c\n\x04\x64\x61ta\x18\x03 \x02(\x0c\x12\n\n\x02iv\x18\x04 \x01(\x0c\x12\x0c\n\x04hash\x18\x05 \x01(\x0c\x12\x1b\n\x0ehash_algorithm\x18\x06 \x01(\t:\x03MD5BJ\n1org.apache.hadoop.hbase.shaded.protobuf.generatedB\x10\x45ncryptionProtosH\x01\xa0\x01\x01')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'Encryption_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  _globals['DESCRIPTOR']._options = None
  _globals['DESCRIPTOR']._serialized_options = b'\n1org.apache.hadoop.hbase.shaded.protobuf.generatedB\020EncryptionProtosH\001\240\001\001'
  _globals['_WRAPPEDKEY']._serialized_start=30
  _globals['_WRAPPEDKEY']._serialized_end=146
# @@protoc_insertion_point(module_scope)