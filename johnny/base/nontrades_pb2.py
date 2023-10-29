# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: johnny/base/nontrades.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from johnny.base import common_pb2 as johnny_dot_base_dot_common__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1bjohnny/base/nontrades.proto\x12\x06johnny\x1a\x18johnny/base/common.proto\"\x95\x04\n\x08NonTrade\x12)\n\x07rowtype\x18\x01 \x02(\x0e\x32\x18.johnny.NonTrade.RowType\x12\x12\n\nnativetype\x18\x02 \x01(\t\x12\x0f\n\x07\x61\x63\x63ount\x18\x03 \x02(\t\x12\x16\n\x0etransaction_id\x18\x04 \x02(\t\x12\"\n\x08\x64\x61tetime\x18\x05 \x02(\x0b\x32\x10.johnny.DateTime\x12\x13\n\x0b\x64\x65scription\x18\x06 \x02(\t\x12\x0e\n\x06symbol\x18\x07 \x01(\t\x12\x0b\n\x03ref\x18\x08 \x01(\t\x12\x1f\n\x06\x61mount\x18\t \x02(\x0b\x32\x0f.johnny.Decimal\x12 \n\x07\x62\x61lance\x18\n \x01(\x0b\x32\x0f.johnny.Decimal\"\x87\x02\n\x07RowType\x12\x0b\n\x07\x42\x61lance\x10\x01\x12\x12\n\x0e\x46uturesBalance\x10\x02\x12\x0e\n\nAdjustment\x10\x03\x12\x17\n\x13\x46uturesMarkToMarket\x10\x04\x12\x12\n\x0e\x43reditInterest\x10\x05\x12\x12\n\x0eMarginInterest\x10\x06\x12\x14\n\x10InternalTransfer\x10\x07\x12\x14\n\x10\x45xternalTransfer\x10\x08\x12\x0f\n\x0bTransferFee\x10\t\x12\x0e\n\nMonthlyFee\x10\n\x12\x0b\n\x07\x44\x61taFee\x10\x0b\x12\x13\n\x0fHardToBorrowFee\x10\x0c\x12\t\n\x05Sweep\x10\r\x12\x10\n\x0c\x46uturesSweep\x10\x0e')



_NONTRADE = DESCRIPTOR.message_types_by_name['NonTrade']
_NONTRADE_ROWTYPE = _NONTRADE.enum_types_by_name['RowType']
NonTrade = _reflection.GeneratedProtocolMessageType('NonTrade', (_message.Message,), {
  'DESCRIPTOR' : _NONTRADE,
  '__module__' : 'johnny.base.nontrades_pb2'
  # @@protoc_insertion_point(class_scope:johnny.NonTrade)
  })
_sym_db.RegisterMessage(NonTrade)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _NONTRADE._serialized_start=66
  _NONTRADE._serialized_end=599
  _NONTRADE_ROWTYPE._serialized_start=336
  _NONTRADE_ROWTYPE._serialized_end=599
# @@protoc_insertion_point(module_scope)
