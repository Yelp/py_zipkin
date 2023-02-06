from enum import Enum


class Encoding(Enum):
    """Output encodings."""

    V1_THRIFT = "V1_THRIFT"  # No longer supported
    V1_JSON = "V1_JSON"
    V2_JSON = "V2_JSON"
    V2_PROTO3 = "V2_PROTO3"


class Kind(Enum):
    """Type of Span."""

    CLIENT = "CLIENT"
    SERVER = "SERVER"
    PRODUCER = "PRODUCER"
    CONSUMER = "CONSUMER"
    LOCAL = None
