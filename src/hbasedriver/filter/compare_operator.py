from enum import Enum


class CompareOperator(Enum):
    LESS = 1
    LESS_OR_EQUAL = 2
    EQUAL = 3
    NOT_EQUAL = 4
    GREATER_OR_EQUAL = 5
    GREATER = 6
    NO_OP = 7
