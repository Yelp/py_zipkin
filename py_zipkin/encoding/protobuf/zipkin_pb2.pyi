from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Annotation(_message.Message):
    __slots__ = ["timestamp", "value"]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    value: str
    def __init__(self, timestamp: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...

class Endpoint(_message.Message):
    __slots__ = ["ipv4", "ipv6", "port", "service_name"]
    IPV4_FIELD_NUMBER: _ClassVar[int]
    IPV6_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    SERVICE_NAME_FIELD_NUMBER: _ClassVar[int]
    ipv4: bytes
    ipv6: bytes
    port: int
    service_name: str
    def __init__(self, service_name: _Optional[str] = ..., ipv4: _Optional[bytes] = ..., ipv6: _Optional[bytes] = ..., port: _Optional[int] = ...) -> None: ...

class ListOfSpans(_message.Message):
    __slots__ = ["spans"]
    SPANS_FIELD_NUMBER: _ClassVar[int]
    spans: _containers.RepeatedCompositeFieldContainer[Span]
    def __init__(self, spans: _Optional[_Iterable[_Union[Span, _Mapping]]] = ...) -> None: ...

class ReportResponse(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Span(_message.Message):
    __slots__ = ["annotations", "debug", "duration", "id", "kind", "local_endpoint", "name", "parent_id", "remote_endpoint", "shared", "tags", "timestamp", "trace_id"]
    class Kind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    class TagsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ANNOTATIONS_FIELD_NUMBER: _ClassVar[int]
    CLIENT: Span.Kind
    CONSUMER: Span.Kind
    DEBUG_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    LOCAL_ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PARENT_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCER: Span.Kind
    REMOTE_ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    SERVER: Span.Kind
    SHARED_FIELD_NUMBER: _ClassVar[int]
    SPAN_KIND_UNSPECIFIED: Span.Kind
    TAGS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    annotations: _containers.RepeatedCompositeFieldContainer[Annotation]
    debug: bool
    duration: int
    id: bytes
    kind: Span.Kind
    local_endpoint: Endpoint
    name: str
    parent_id: bytes
    remote_endpoint: Endpoint
    shared: bool
    tags: _containers.ScalarMap[str, str]
    timestamp: int
    trace_id: bytes
    def __init__(self, trace_id: _Optional[bytes] = ..., parent_id: _Optional[bytes] = ..., id: _Optional[bytes] = ..., kind: _Optional[_Union[Span.Kind, str]] = ..., name: _Optional[str] = ..., timestamp: _Optional[int] = ..., duration: _Optional[int] = ..., local_endpoint: _Optional[_Union[Endpoint, _Mapping]] = ..., remote_endpoint: _Optional[_Union[Endpoint, _Mapping]] = ..., annotations: _Optional[_Iterable[_Union[Annotation, _Mapping]]] = ..., tags: _Optional[_Mapping[str, str]] = ..., debug: bool = ..., shared: bool = ...) -> None: ...
