# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: johnny/sources/ameritrade/config.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n&johnny/sources/ameritrade/config.proto\x12\x11johnny.ameritrade\"\xd2\x01\n\x06\x43onfig\x12\x36\n.thinkorswim_account_statement_csv_file_pattern\x18\x01 \x01(\t\x12\x38\n0thinkorswim_positions_statement_csv_file_pattern\x18\x02 \x01(\t\x12\x37\n/ameritrade_download_transactions_for_treasuries\x18\x03 \x01(\t\x12\x1d\n\x15schwab_migration_date\x18\x04 \x01(\t')



_CONFIG = DESCRIPTOR.message_types_by_name['Config']
Config = _reflection.GeneratedProtocolMessageType('Config', (_message.Message,), {
  'DESCRIPTOR' : _CONFIG,
  '__module__' : 'johnny.sources.ameritrade.config_pb2'
  # @@protoc_insertion_point(class_scope:johnny.ameritrade.Config)
  })
_sym_db.RegisterMessage(Config)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _CONFIG._serialized_start=62
  _CONFIG._serialized_end=272
# @@protoc_insertion_point(module_scope)
