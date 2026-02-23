"""
Custom exception hierarchy for hbase-driver.

Provides specific exception types for different failure scenarios
with helpful error messages.
"""


class HBaseException(Exception):
    """Base exception for all hbase-driver errors."""

    def __init__(self, msg: str = None):
        super().__init__(msg)
        self.msg = msg

    def __str__(self):
        return f"HBaseException: {self.msg}" if self.msg else "HBaseException"


class ConnectionException(HBaseException):
    """Exception raised when connection to HBase cluster fails."""

    def __str__(self):
        return f"ConnectionException: {self.msg}" if self.msg else "ConnectionException"


class ZooKeeperException(HBaseException):
    """Exception raised when ZooKeeper operations fail."""

    def __str__(self):
        return f"ZooKeeperException: {self.msg}" if self.msg else "ZooKeeperException"


class TableNotFoundException(HBaseException):
    """Exception raised when the requested table does not exist."""

    def __str__(self):
        return f"TableNotFoundException: {self.msg}" if self.msg else "TableNotFoundException"


class TableDisabledException(HBaseException):
    """Exception raised when operations are attempted on a disabled table."""

    def __str__(self):
        return f"TableDisabledException: {self.msg}" if self.msg else "TableDisabledException"


class RegionException(HBaseException):
    """Exception raised when region operations fail."""

    def __str__(self):
        return f"RegionException: {self.msg}" if self.msg else "RegionException"


class SerializationException(HBaseException):
    """Exception raised when data serialization/deserialization fails."""

    def __str__(self):
        return f"SerializationException: {self.msg}" if self.msg else "SerializationException"


class TimeoutException(HBaseException):
    """Exception raised when an operation times out."""

    def __str__(self):
        return f"TimeoutException: {self.msg}" if self.msg else "TimeoutException"


class ValidationException(HBaseException):
    """Exception raised when input validation fails."""

    def __str__(self):
        return f"ValidationException: {self.msg}" if self.msg else "ValidationException"


class FilterException(HBaseException):
    """Exception raised when filter operations fail."""

    def __str__(self):
        return f"FilterException: {self.msg}" if self.msg else "FilterException"


class BatchException(HBaseException):
    """Exception raised when batch operations fail."""

    def __str__(self):
        return f"BatchException: {self.msg}" if self.msg else "BatchException"
