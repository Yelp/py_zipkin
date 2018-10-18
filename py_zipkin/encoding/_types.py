# -*- coding: utf-8 -*-
from enum import Enum


class Encoding(Enum):
    """Supported output encodings."""
    V1_THRIFT = 'V1_THRIFT'
    V1_JSON = 'V1_JSON'
    V2_JSON = 'V2_JSON'
    V2_PROTOBUF = 'V2_PROTOBUF'


class Kind(Enum):
    """Type of Span."""
    CLIENT = 'CLIENT'
    SERVER = 'SERVER'
    LOCAL = None
