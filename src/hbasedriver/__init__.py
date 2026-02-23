"""
hbase-driver - Pure Python HBase Client

A native Python implementation of HBase client without Thrift dependency.
Implements core HBase RegionServer and Master RPC protocols.
"""

# Import main API classes
from hbasedriver.client.client import Client
from hbasedriver.client.admin import Admin
from hbasedriver.client.table import Table
from hbasedriver.client.result_scanner import ResultScanner
from hbasedriver.client.cluster_connection import ClusterConnection

# Import operations
from hbasedriver.operations.put import Put
from hbasedriver.operations.get import Get
from hbasedriver.operations.delete import Delete
from hbasedriver.operations.scan import Scan
from hbasedriver.operations.batch import BatchGet, BatchPut
from hbasedriver.operations.increment import Increment, CheckAndPut

# Import filters
from hbasedriver.filter.filter import Filter, ReturnCode
from hbasedriver.filter.filter_base import FilterBase
from hbasedriver.filter.compare_filter import CompareFilter
from hbasedriver.filter.compare_operator import CompareOperator
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

# Import comparators
from hbasedriver.filter.byte_array_comparable import ByteArrayComparable
from hbasedriver.filter.binary_comparator import BinaryComparator
from hbasedriver.filter.binary_prefix_comparator import BinaryPrefixComparator

# Import model classes
from hbasedriver.model import Cell, Row, CellType

# Import utilities
from hbasedriver.table_name import TableName

# Import constants
from hbasedriver.hbase_constants import HConstants

# Import exceptions from hbase_exceptions subdirectory
from hbasedriver.hbase_exceptions import (
    HBaseException,
    ConnectionException,
    ZooKeeperException,
    TableNotFoundException,
    TableDisabledException,
    RegionException,
    SerializationException,
    TimeoutException,
    ValidationException,
    FilterException,
    BatchException,
)

__all__ = [
    # Client classes
    "Client",
    "Admin",
    "Table",
    "ResultScanner",
    "ClusterConnection",
    # Operations
    "Put",
    "Get",
    "Delete",
    "Scan",
    "BatchGet",
    "BatchPut",
    "Increment",
    "CheckAndPut",
    # Filters
    "Filter",
    "ReturnCode",
    "FilterBase",
    "CompareFilter",
    "CompareOperator",
    "FilterList",
    "FilterListOperator",
    "RowFilter",
    "FamilyFilter",
    "QualifierFilter",
    "ValueFilter",
    "PrefixFilter",
    "PageFilter",
    "KeyOnlyFilter",
    "ColumnPrefixFilter",
    "SingleColumnValueFilter",
    "FirstKeyOnlyFilter",
    "MultipleColumnPrefixFilter",
    "TimestampsFilter",
    # Comparators
    "ByteArrayComparable",
    "BinaryComparator",
    "BinaryPrefixComparator",
    # Model
    "Cell",
    "Row",
    "CellType",
    # Utilities
    "TableName",
    # Constants
    "HConstants",
    # Exceptions
    "HBaseException",
    "ConnectionException",
    "ZooKeeperException",
    "TableNotFoundException",
    "TableDisabledException",
    "RegionException",
    "SerializationException",
    "TimeoutException",
    "ValidationException",
    "FilterException",
    "BatchException",
]
