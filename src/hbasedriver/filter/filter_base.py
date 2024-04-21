from typing import List

from hbasedriver.filter.filter import Filter
from hbasedriver.model import Cell


class FilterBase(Filter):
    def reset(self) -> None:
        pass

    def filterRowKey(self, cell: Cell) -> bool:
        if self.filterAllRemaining():
            return True
        return False

    def filterAllRemaining(self) -> bool:
        return False

    def transformCell(self, v: Cell) -> Cell:
        return v

    def filterRowCells(self, ignored: List[Cell]) -> None:
        pass

    def hasFilterRow(self) -> bool:
        return False

    def filterRow(self) -> bool:
        return False

    def getNextCellHint(self, currentCell: Cell) -> Cell:
        return None

    def isFamilyEssential(self, name: bytes) -> bool:
        return True

    @staticmethod
    def createFilterFromArguments(filterArguments: List[bytes]) -> Filter:
        raise NotImplementedError("This method has not been implemented")

    def __str__(self) -> str:
        return self.__class__.__name__

    def toByteArray(self) -> bytes:
        return bytes()

    def areSerializedFieldsEqual(self, other: Filter) -> bool:
        return True
