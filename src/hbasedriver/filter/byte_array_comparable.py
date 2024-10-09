from abc import ABC, abstractmethod


class ByteArrayComparable(ABC):

    def __init__(self, value: bytes):
        self.value = value

    # subclass should override this to return exact JAVA class name for server to locate the handle class.
    @abstractmethod
    def get_name(self):
        pass

    def get_value(self) -> bytes:
        return self.value

    @abstractmethod
    def to_byte_array(self) -> bytes:
        pass

    @abstractmethod
    def compare_to(self, value: bytes, offset: int = 0, length: int = None) -> int:
        pass

    @abstractmethod
    def are_serialized_fields_equal(self, other: 'ByteArrayComparable') -> bool:
        pass
