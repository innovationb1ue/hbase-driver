from abc import ABC, abstractmethod


class ByteArrayComparable(ABC):

    def __init__(self, value: bytes):
        self.value = value

    def getValue(self) -> bytes:
        return self.value

    @abstractmethod
    def toByteArray(self) -> bytes:
        pass

    @abstractmethod
    def compareTo(self, value: bytes, offset: int = 0, length: int = None) -> int:
        pass

    @abstractmethod
    def areSerializedFieldsEqual(self, other: 'ByteArrayComparable') -> bool:
        pass
