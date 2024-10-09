from hbasedriver.filter.compare_operator import CompareOperator
from hbasedriver.filter.parse_constant import ParseConstants


def create_compare_operator(compare_op_as_byte_array: bytes) -> 'CompareOperator':
    if compare_op_as_byte_array == ParseConstants.LESS_THAN_BUFFER:
        return CompareOperator.LESS
    elif compare_op_as_byte_array == ParseConstants.LESS_THAN_OR_EQUAL_TO_BUFFER:
        return CompareOperator.LESS_OR_EQUAL
    elif compare_op_as_byte_array == ParseConstants.GREATER_THAN_BUFFER:
        return CompareOperator.GREATER
    elif compare_op_as_byte_array == ParseConstants.GREATER_THAN_OR_EQUAL_TO_BUFFER:
        return CompareOperator.GREATER_OR_EQUAL
    elif compare_op_as_byte_array == ParseConstants.NOT_EQUAL_TO_BUFFER:
        return CompareOperator.NOT_EQUAL
    elif compare_op_as_byte_array == ParseConstants.EQUAL_TO_BUFFER:
        return CompareOperator.EQUAL
    else:
        raise ValueError("Invalid compare operator")
