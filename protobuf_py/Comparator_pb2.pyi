from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Comparator(_message.Message):
    __slots__ = ("name", "serialized_comparator")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SERIALIZED_COMPARATOR_FIELD_NUMBER: _ClassVar[int]
    name: str
    serialized_comparator: bytes
    def __init__(self, name: _Optional[str] = ..., serialized_comparator: _Optional[bytes] = ...) -> None: ...

class ByteArrayComparable(_message.Message):
    __slots__ = ("value",)
    VALUE_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    def __init__(self, value: _Optional[bytes] = ...) -> None: ...

class BinaryComparator(_message.Message):
    __slots__ = ("comparable",)
    COMPARABLE_FIELD_NUMBER: _ClassVar[int]
    comparable: ByteArrayComparable
    def __init__(self, comparable: _Optional[_Union[ByteArrayComparable, _Mapping]] = ...) -> None: ...

class LongComparator(_message.Message):
    __slots__ = ("comparable",)
    COMPARABLE_FIELD_NUMBER: _ClassVar[int]
    comparable: ByteArrayComparable
    def __init__(self, comparable: _Optional[_Union[ByteArrayComparable, _Mapping]] = ...) -> None: ...

class BinaryPrefixComparator(_message.Message):
    __slots__ = ("comparable",)
    COMPARABLE_FIELD_NUMBER: _ClassVar[int]
    comparable: ByteArrayComparable
    def __init__(self, comparable: _Optional[_Union[ByteArrayComparable, _Mapping]] = ...) -> None: ...

class BitComparator(_message.Message):
    __slots__ = ("comparable", "bitwise_op")
    class BitwiseOp(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        AND: _ClassVar[BitComparator.BitwiseOp]
        OR: _ClassVar[BitComparator.BitwiseOp]
        XOR: _ClassVar[BitComparator.BitwiseOp]
    AND: BitComparator.BitwiseOp
    OR: BitComparator.BitwiseOp
    XOR: BitComparator.BitwiseOp
    COMPARABLE_FIELD_NUMBER: _ClassVar[int]
    BITWISE_OP_FIELD_NUMBER: _ClassVar[int]
    comparable: ByteArrayComparable
    bitwise_op: BitComparator.BitwiseOp
    def __init__(self, comparable: _Optional[_Union[ByteArrayComparable, _Mapping]] = ..., bitwise_op: _Optional[_Union[BitComparator.BitwiseOp, str]] = ...) -> None: ...

class NullComparator(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RegexStringComparator(_message.Message):
    __slots__ = ("pattern", "pattern_flags", "charset", "engine")
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FLAGS_FIELD_NUMBER: _ClassVar[int]
    CHARSET_FIELD_NUMBER: _ClassVar[int]
    ENGINE_FIELD_NUMBER: _ClassVar[int]
    pattern: str
    pattern_flags: int
    charset: str
    engine: str
    def __init__(self, pattern: _Optional[str] = ..., pattern_flags: _Optional[int] = ..., charset: _Optional[str] = ..., engine: _Optional[str] = ...) -> None: ...

class SubstringComparator(_message.Message):
    __slots__ = ("substr",)
    SUBSTR_FIELD_NUMBER: _ClassVar[int]
    substr: str
    def __init__(self, substr: _Optional[str] = ...) -> None: ...

class BigDecimalComparator(_message.Message):
    __slots__ = ("comparable",)
    COMPARABLE_FIELD_NUMBER: _ClassVar[int]
    comparable: ByteArrayComparable
    def __init__(self, comparable: _Optional[_Union[ByteArrayComparable, _Mapping]] = ...) -> None: ...

class BinaryComponentComparator(_message.Message):
    __slots__ = ("value", "offset")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    value: bytes
    offset: int
    def __init__(self, value: _Optional[bytes] = ..., offset: _Optional[int] = ...) -> None: ...
