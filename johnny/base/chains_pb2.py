# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: johnny/base/chains.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='johnny/base/chains.proto',
  package='johnny',
  syntax='proto2',
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x18johnny/base/chains.proto\x12\x06johnny\"\'\n\x06\x43hains\x12\x1d\n\x06\x63hains\x18\x01 \x03(\x0b\x32\r.johnny.Chain\"\xcf\x01\n\x05\x43hain\x12\x10\n\x08\x63hain_id\x18\x01 \x01(\t\x12#\n\x06status\x18\x02 \x01(\x0e\x32\x13.johnny.ChainStatus\x12\r\n\x05group\x18\x03 \x01(\t\x12\x10\n\x08strategy\x18\x04 \x01(\t\x12\x0f\n\x07\x63omment\x18\x05 \x01(\t\x12\x0b\n\x03pop\x18\x06 \x01(\x02\x12\x0e\n\x06target\x18\x07 \x01(\x02\x12\x0f\n\x07pnl_win\x18\x08 \x01(\x02\x12\x10\n\x08pnl_loss\x18\t \x01(\x02\x12\x0b\n\x03ids\x18\n \x03(\t\x12\x10\n\x08\x61uto_ids\x18\x0b \x03(\t\":\n\x05\x41sset\x12\x12\n\ninstrument\x18\x01 \x01(\t\x12\r\n\x05\x63lass\x18\x02 \x01(\t\x12\x0e\n\x06\x66\x61\x63tor\x18\x03 \x01(\t*<\n\x0b\x43hainStatus\x12\n\n\x06\x41\x43TIVE\x10\x00\x12\n\n\x06\x43LOSED\x10\x01\x12\t\n\x05\x46INAL\x10\x02\x12\n\n\x06IGNORE\x10\x03'
)

_CHAINSTATUS = _descriptor.EnumDescriptor(
  name='ChainStatus',
  full_name='johnny.ChainStatus',
  filename=None,
  file=DESCRIPTOR,
  create_key=_descriptor._internal_create_key,
  values=[
    _descriptor.EnumValueDescriptor(
      name='ACTIVE', index=0, number=0,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='CLOSED', index=1, number=1,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='FINAL', index=2, number=2,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
    _descriptor.EnumValueDescriptor(
      name='IGNORE', index=3, number=3,
      serialized_options=None,
      type=None,
      create_key=_descriptor._internal_create_key),
  ],
  containing_type=None,
  serialized_options=None,
  serialized_start=347,
  serialized_end=407,
)
_sym_db.RegisterEnumDescriptor(_CHAINSTATUS)

ChainStatus = enum_type_wrapper.EnumTypeWrapper(_CHAINSTATUS)
ACTIVE = 0
CLOSED = 1
FINAL = 2
IGNORE = 3



_CHAINS = _descriptor.Descriptor(
  name='Chains',
  full_name='johnny.Chains',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='chains', full_name='johnny.Chains.chains', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=36,
  serialized_end=75,
)


_CHAIN = _descriptor.Descriptor(
  name='Chain',
  full_name='johnny.Chain',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='chain_id', full_name='johnny.Chain.chain_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='status', full_name='johnny.Chain.status', index=1,
      number=2, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='group', full_name='johnny.Chain.group', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='strategy', full_name='johnny.Chain.strategy', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='comment', full_name='johnny.Chain.comment', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='pop', full_name='johnny.Chain.pop', index=5,
      number=6, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='target', full_name='johnny.Chain.target', index=6,
      number=7, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='pnl_win', full_name='johnny.Chain.pnl_win', index=7,
      number=8, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='pnl_loss', full_name='johnny.Chain.pnl_loss', index=8,
      number=9, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='ids', full_name='johnny.Chain.ids', index=9,
      number=10, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='auto_ids', full_name='johnny.Chain.auto_ids', index=10,
      number=11, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=78,
  serialized_end=285,
)


_ASSET = _descriptor.Descriptor(
  name='Asset',
  full_name='johnny.Asset',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='instrument', full_name='johnny.Asset.instrument', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='class', full_name='johnny.Asset.class', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='factor', full_name='johnny.Asset.factor', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=287,
  serialized_end=345,
)

_CHAINS.fields_by_name['chains'].message_type = _CHAIN
_CHAIN.fields_by_name['status'].enum_type = _CHAINSTATUS
DESCRIPTOR.message_types_by_name['Chains'] = _CHAINS
DESCRIPTOR.message_types_by_name['Chain'] = _CHAIN
DESCRIPTOR.message_types_by_name['Asset'] = _ASSET
DESCRIPTOR.enum_types_by_name['ChainStatus'] = _CHAINSTATUS
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

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


# @@protoc_insertion_point(module_scope)
