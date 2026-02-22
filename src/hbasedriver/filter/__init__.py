"""
HBase Filter implementations.

This module provides Python implementations of HBase filters that are
serialized and sent to the HBase region servers for execution.

Available filters:
- FilterList: Combine multiple filters with AND/OR logic
- RowFilter: Filter rows based on row key comparison
- FamilyFilter: Filter based on column family name
- QualifierFilter: Filter based on column qualifier name
- ValueFilter: Filter based on cell value
- PrefixFilter: Filter rows by row key prefix
- PageFilter: Limit number of rows per page
- KeyOnlyFilter: Return only row keys (no values)
- ColumnPrefixFilter: Filter columns by qualifier prefix
- SingleColumnValueFilter: Filter rows based on a specific column's value
- FirstKeyOnlyFilter: Return only the first column of each row
- MultipleColumnPrefixFilter: Filter columns by multiple prefixes
- TimestampsFilter: Filter cells by specific timestamps

Comparators:
- BinaryComparator: Compare byte arrays
- BinaryPrefixComparator: Compare byte array prefixes
"""

# Base classes
from hbasedriver.filter.filter import Filter, ReturnCode
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.filter.compare_filter import CompareFilter
from hbasedriver.filter.compare_operator import CompareOperator

# Comparators
from hbasedriver.filter.byte_array_comparable import ByteArrayComparable
from hbasedriver.filter.binary_comparator import BinaryComparator
from hbasedriver.filter.binary_prefix_comparator import BinaryPrefixComparator

# Filters
from hbasedriver.filter.filter_list import FilterList
from hbasedriver.filter.rowfilter import RowFilter
from hbasedriver.filter.family_filter import FamilyFilter
from hbasedriver.filter.qualifier_filter import QualifierFilter
from hbasedriver.filter.value_filter import ValueFilter
from hbasedriver.filter.prefix_filter import PrefixFilter
from hbasedriver.filter.page_filter import PageFilter
from hbasedriver.filter.key_only_filter import KeyOnlyFilter
from hbasedriver.filter.column_prefix_filter import ColumnPrefixFilter
from hbasedriver.filter.single_column_value_filter import SingleColumnValueFilter
from hbasedriver.filter.first_key_only_filter import FirstKeyOnlyFilter
from hbasedriver.filter.multiple_column_prefix_filter import MultipleColumnPrefixFilter
from hbasedriver.filter.timestamps_filter import TimestampsFilter

__all__ = [
    # Base classes
    'Filter',
    'ReturnCode',
    'FilterBase',
    'CompareFilter',
    'CompareOperator',
    # Comparators
    'ByteArrayComparable',
    'BinaryComparator',
    'BinaryPrefixComparator',
    # Filters
    'FilterList',
    'RowFilter',
    'FamilyFilter',
    'QualifierFilter',
    'ValueFilter',
    'PrefixFilter',
    'PageFilter',
    'KeyOnlyFilter',
    'ColumnPrefixFilter',
    'SingleColumnValueFilter',
    'FirstKeyOnlyFilter',
    'MultipleColumnPrefixFilter',
    'TimestampsFilter',
]
