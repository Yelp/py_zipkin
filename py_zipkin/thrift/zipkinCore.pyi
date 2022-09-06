from dataclasses import dataclass
from enum import IntEnum
from typing import *

class AnnotationType(IntEnum):
    BOOL = 0
    BYTES = 1
    I16 = 2
    I32 = 3
    I64 = 4
    DOUBLE = 5
    STRING = 6

@dataclass
class Endpoint:
    ipv4: Optional[int] = None
    port: Optional[int] = None
    service_name: Optional[str] = None
    ipv6: Optional[str] = None

@dataclass
class Annotation:
    timestamp: Optional[int] = None
    value: Optional[str] = None
    host: Optional[Endpoint] = None

@dataclass
class BinaryAnnotation:
    key: Optional[str] = None
    value: Optional[str] = None
    annotation_type: Optional[int] = None
    host: Optional[Endpoint] = None

@dataclass
class Span:
    trace_id: Optional[int] = None
    name: Optional[str] = None
    id: Optional[int] = None
    parent_id: Optional[int] = None
    annotations: Optional[List[Annotation]] = None
    binary_annotations: Optional[List[BinaryAnnotation]] = None
    debug: Optional[bool] = False
    timestamp: Optional[int] = None
    duration: Optional[int] = None
    trace_id_high: Optional[int] = None
