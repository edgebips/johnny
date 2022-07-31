# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: johnny/base/chains.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x18johnny/base/chains.proto\x12\x06johnny\"]\n\x06\x43hains\x12\x1d\n\x06\x63hains\x18\x01 \x03(\x0b\x32\r.johnny.Chain\x12\x34\n\x12split_transactions\x18\x02 \x03(\x0b\x32\x18.johnny.SplitTransaction\"\xbb\x02\n\x05\x43hain\x12\x10\n\x08\x63hain_id\x18\x01 \x01(\t\x12#\n\x06status\x18\x02 \x01(\x0e\x32\x13.johnny.ChainStatus\x12\r\n\x05group\x18\x03 \x01(\t\x12\x10\n\x08strategy\x18\x04 \x01(\t\x12\x0c\n\x04tags\x18\x05 \x03(\t\x12\x0f\n\x07\x63omment\x18\x06 \x01(\t\x12\x13\n\x0bvol_implied\x18\x10 \x01(\x02\x12\x14\n\x0cvol_realized\x18\x11 \x01(\x02\x12\x0b\n\x03pop\x18\x07 \x01(\x02\x12\x0e\n\x06target\x18\x08 \x01(\x02\x12\r\n\x05xrefs\x18\t \x03(\t\x12\x0f\n\x07join_id\x18\x0e \x01(\t\x12\x11\n\tlong_term\x18\x0f \x01(\x08\x12\x0f\n\x07pnl_win\x18\n \x01(\x02\x12\x10\n\x08pnl_loss\x18\x0b \x01(\x02\x12\x0b\n\x03ids\x18\x0c \x03(\t\x12\x10\n\x08\x61uto_ids\x18\r \x03(\t\":\n\x05\x41sset\x12\x12\n\ninstrument\x18\x01 \x01(\t\x12\r\n\x05\x63lass\x18\x02 \x01(\t\x12\x0e\n\x06\x66\x61\x63tor\x18\x03 \x01(\t\"r\n\x10SplitTransaction\x12\n\n\x02id\x18\x01 \x01(\t\x12,\n\x05parts\x18\x02 \x03(\x0b\x32\x1d.johnny.SplitTransaction.Part\x1a$\n\x04Part\x12\n\n\x02id\x18\x01 \x01(\t\x12\x10\n\x08quantity\x18\x02 \x01(\x05*<\n\x0b\x43hainStatus\x12\n\n\x06\x41\x43TIVE\x10\x00\x12\n\n\x06\x43LOSED\x10\x01\x12\t\n\x05\x46INAL\x10\x02\x12\n\n\x06IGNORE\x10\x03')

_CHAINSTATUS = DESCRIPTOR.enum_types_by_name['ChainStatus']
ChainStatus = enum_type_wrapper.EnumTypeWrapper(_CHAINSTATUS)
ACTIVE = 0
CLOSED = 1
FINAL = 2
IGNORE = 3


_CHAINS = DESCRIPTOR.message_types_by_name['Chains']
_CHAIN = DESCRIPTOR.message_types_by_name['Chain']
_ASSET = DESCRIPTOR.message_types_by_name['Asset']
_SPLITTRANSACTION = DESCRIPTOR.message_types_by_name['SplitTransaction']
_SPLITTRANSACTION_PART = _SPLITTRANSACTION.nested_types_by_name['Part']
Chains = _reflection.GeneratedProtocolMessageType('Chains', (_message.Message,), {
  'DESCRIPTOR' : _CHAINS,
  '__module__' : 'johnny.base.chains_pb2'
  # @@protoc_insertion_point(class_scope:johnny.Chains)
  })
_sym_db.RegisterMessage(Chains)

Chain = _reflection.GeneratedProtocolMessageType('Chain', (_message.Message,), {
  'DESCRIPTOR' : _CHAIN,
  '__module__' : 'johnny.base.chains_pb2'
  # @@protoc_insertion_point(class_scope:johnny.Chain)
  })
_sym_db.RegisterMessage(Chain)

Asset = _reflection.GeneratedProtocolMessageType('Asset', (_message.Message,), {
  'DESCRIPTOR' : _ASSET,
  '__module__' : 'johnny.base.chains_pb2'
  # @@protoc_insertion_point(class_scope:johnny.Asset)
  })
_sym_db.RegisterMessage(Asset)

SplitTransaction = _reflection.GeneratedProtocolMessageType('SplitTransaction', (_message.Message,), {

  'Part' : _reflection.GeneratedProtocolMessageType('Part', (_message.Message,), {
    'DESCRIPTOR' : _SPLITTRANSACTION_PART,
    '__module__' : 'johnny.base.chains_pb2'
    # @@protoc_insertion_point(class_scope:johnny.SplitTransaction.Part)
    })
  ,
  'DESCRIPTOR' : _SPLITTRANSACTION,
  '__module__' : 'johnny.base.chains_pb2'
  # @@protoc_insertion_point(class_scope:johnny.SplitTransaction)
  })
_sym_db.RegisterMessage(SplitTransaction)
_sym_db.RegisterMessage(SplitTransaction.Part)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _CHAINSTATUS._serialized_start=625
  _CHAINSTATUS._serialized_end=685
  _CHAINS._serialized_start=36
  _CHAINS._serialized_end=129
  _CHAIN._serialized_start=132
  _CHAIN._serialized_end=447
  _ASSET._serialized_start=449
  _ASSET._serialized_end=507
  _SPLITTRANSACTION._serialized_start=509
  _SPLITTRANSACTION._serialized_end=623
  _SPLITTRANSACTION_PART._serialized_start=587
  _SPLITTRANSACTION_PART._serialized_end=623
# @@protoc_insertion_point(module_scope)
