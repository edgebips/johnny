# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: johnny/base/taxes.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x17johnny/base/taxes.proto\x12\x06johnny\"\t\n\x07\x44\x65\x63imal\"\xe0\x01\n\tWorksheet\x12\x0f\n\x07instype\x18\x01 \x02(\t\x12\x0e\n\x06symbol\x18\x02 \x02(\t\x12\x1d\n\x04\x63ost\x18\x03 \x02(\x0b\x32\x0f.johnny.Decimal\x12!\n\x08proceeds\x18\x04 \x02(\x0b\x32\x0f.johnny.Decimal\x12%\n\x0cst_gain_loss\x18\x05 \x02(\x0b\x32\x0f.johnny.Decimal\x12%\n\x0clt_gain_loss\x18\x06 \x02(\x0b\x32\x0f.johnny.Decimal\x12\"\n\tgain_loss\x18\x07 \x02(\x0b\x32\x0f.johnny.Decimal\"\xc5\x02\n\x08\x46orm8949\x12\x0f\n\x07instype\x18\x01 \x02(\t\x12\x0e\n\x06symbol\x18\x02 \x02(\t\x12\x1d\n\x04\x63ost\x18\x03 \x02(\x0b\x32\x0f.johnny.Decimal\x12!\n\x08proceeds\x18\x04 \x02(\x0b\x32\x0f.johnny.Decimal\x12!\n\x08gain_adj\x18\x05 \x02(\x0b\x32\x0f.johnny.Decimal\x12\"\n\tgain_loss\x18\x06 \x02(\x0b\x32\x0f.johnny.Decimal\x12#\n\x04term\x18\x07 \x02(\x0e\x32\x15.johnny.Form8949.Term\x12!\n\x03\x62ox\x18\x08 \x01(\x0e\x32\x14.johnny.Form8949.Box\"\x16\n\x04Term\x12\x06\n\x02ST\x10\x01\x12\x06\n\x02LT\x10\x02\"/\n\x03\x42ox\x12\x05\n\x01\x41\x10\x01\x12\x05\n\x01\x42\x10\x02\x12\x05\n\x01\x43\x10\x03\x12\x05\n\x01\x44\x10\x04\x12\x05\n\x01\x45\x10\x05\x12\x05\n\x01\x46\x10\x06')



_DECIMAL = DESCRIPTOR.message_types_by_name['Decimal']
_WORKSHEET = DESCRIPTOR.message_types_by_name['Worksheet']
_FORM8949 = DESCRIPTOR.message_types_by_name['Form8949']
_FORM8949_TERM = _FORM8949.enum_types_by_name['Term']
_FORM8949_BOX = _FORM8949.enum_types_by_name['Box']
Decimal = _reflection.GeneratedProtocolMessageType('Decimal', (_message.Message,), {
  'DESCRIPTOR' : _DECIMAL,
  '__module__' : 'johnny.base.taxes_pb2'
  # @@protoc_insertion_point(class_scope:johnny.Decimal)
  })
_sym_db.RegisterMessage(Decimal)

Worksheet = _reflection.GeneratedProtocolMessageType('Worksheet', (_message.Message,), {
  'DESCRIPTOR' : _WORKSHEET,
  '__module__' : 'johnny.base.taxes_pb2'
  # @@protoc_insertion_point(class_scope:johnny.Worksheet)
  })
_sym_db.RegisterMessage(Worksheet)

Form8949 = _reflection.GeneratedProtocolMessageType('Form8949', (_message.Message,), {
  'DESCRIPTOR' : _FORM8949,
  '__module__' : 'johnny.base.taxes_pb2'
  # @@protoc_insertion_point(class_scope:johnny.Form8949)
  })
_sym_db.RegisterMessage(Form8949)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _DECIMAL._serialized_start=35
  _DECIMAL._serialized_end=44
  _WORKSHEET._serialized_start=47
  _WORKSHEET._serialized_end=271
  _FORM8949._serialized_start=274
  _FORM8949._serialized_end=599
  _FORM8949_TERM._serialized_start=528
  _FORM8949_TERM._serialized_end=550
  _FORM8949_BOX._serialized_start=552
  _FORM8949_BOX._serialized_end=599
# @@protoc_insertion_point(module_scope)
