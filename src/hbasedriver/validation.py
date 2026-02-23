"""
Configuration validation for hbase-driver.

This module provides validation functions for HBase configuration
to fail fast with helpful error messages.
"""

from typing import Dict, List, Optional

from hbasedriver.hbase_constants import HConstants
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


def validate_config(config: Dict[str, any]) -> List[str]:
    """Validate HBase configuration and return list of errors.

    Validates required and optional configuration parameters, providing
    clear error messages for any issues found.

    Args:
        config: Configuration dictionary to validate

    Returns:
        List of error messages. Empty list if configuration is valid.
    """
    errors: List[str] = []

    # Validate required: ZooKeeper quorum
    if HConstants.ZOOKEEPER_QUORUM not in config or not config[HConstants.ZOOKEEPER_QUORUM]:
        errors.append(
            f"Missing required configuration: '{HConstants.ZOOKEEPER_QUORUM}'. "
            "ZooKeeper quorum address is required to connect to HBase cluster. "
            "Example: 'hbase.zookeeper.quorum': 'localhost:2181'"
        )

    # Validate ZooKeeper quorum format
    if HConstants.ZOOKEEPER_QUORUM in config:
        zk_quorum = config[HConstants.ZOOKEEPER_QUORUM]
        if not isinstance(zk_quorum, (str, list)):
            errors.append(
                f"Invalid configuration: '{HConstants.ZOOKEEPER_QUORUM}' must be a string "
                "or comma-separated list of host:port pairs. "
                f"Got: {type(zk_quorum).__name__}"
            )
        else:
            # Check format of each quorum entry
            if isinstance(zk_quorum, str):
                entries = [e.strip() for e in zk_quorum.split(",")]
            else:
                entries = zk_quorum

            for entry in entries:
                if ":" not in entry:
                    # Check if default port should be assumed
                    continue
                parts = entry.split(":")
                if len(parts) != 2:
                    errors.append(
                        f"Invalid ZooKeeper address: '{entry}'. "
                        "Must be in 'host:port' format. "
                        f"Got: {entry}"
                    )
                else:
                    host, port = parts
                    # Validate port is numeric
                    try:
                        port_num = int(port)
                        if port_num < 1 or port_num > 65535:
                            errors.append(
                                f"Invalid port in '{entry}': {port_num}. "
                                "Port must be between 1 and 65535."
                            )
                    except ValueError:
                        errors.append(
                            f"Invalid port in '{entry}': '{port}'. "
                            "Port must be a numeric value."
                        )

    # Validate connection pool size
    if HConstants.CONNECTION_POOL_SIZE in config:
        pool_size = config[HConstants.CONNECTION_POOL_SIZE]
        if not isinstance(pool_size, int):
            errors.append(
                f"Invalid configuration: '{HConstants.CONNECTION_POOL_SIZE}' must be an integer. "
                f"Got: {type(pool_size).__name__}"
            )
        elif pool_size < 1:
            errors.append(
                f"Invalid connection pool size: {pool_size}. "
                "Pool size must be at least 1."
            )
        elif pool_size > 1000:
            errors.append(
                f"Invalid connection pool size: {pool_size}. "
                "Pool size should not exceed 1000."
            )

    # Validate idle timeout
    if HConstants.CONNECTION_IDLE_TIMEOUT in config:
        idle_timeout = config[HConstants.CONNECTION_IDLE_TIMEOUT]
        if not isinstance(idle_timeout, int):
            errors.append(
                f"Invalid configuration: '{HConstants.CONNECTION_IDLE_TIMEOUT}' must be an integer. "
                f"Got: {type(idle_timeout).__name__}"
            )
        elif idle_timeout < 0:
            errors.append(
                f"Invalid idle timeout: {idle_timeout}. "
                "Timeout must be a non-negative integer (seconds)."
            )

    # Validate ZooKeeper session timeout
    if HConstants.ZOOKEEPER_SESSION_TIMEOUT in config:
        session_timeout = config[HConstants.ZOOKEEPER_SESSION_TIMEOUT]
        if not isinstance(session_timeout, int):
            errors.append(
                f"Invalid configuration: '{HConstants.ZOOKEEPER_SESSION_TIMEOUT}' must be an integer. "
                f"Got: {type(session_timeout).__name__}"
            )
        elif session_timeout < 1000:
            errors.append(
                f"Invalid session timeout: {session_timeout}. "
                "Session timeout should be at least 1000ms (1 second)."
            )

    # Validate ZNode parent
    if HConstants.ZOOKEEPER_ZNODE_PARENT in config:
        znode_parent = config[HConstants.ZOOKEEPER_ZNODE_PARENT]
        if not isinstance(znode_parent, (str, bytes)):
            errors.append(
                f"Invalid configuration: '{HConstants.ZOOKEEPER_ZNODE_PARENT}' must be a string or bytes. "
                f"Got: {type(znode_parent).__name__}"
            )

    return errors


def validate_namespace(ns: any) -> List[str]:
    """Validate HBase namespace.

    Args:
        ns: Namespace value to validate (str or bytes)

    Returns:
        List of error messages. Empty list if valid.
    """
    errors: List[str] = []

    if ns is None:
        errors.append("Namespace cannot be None.")
        return errors

    # Convert to bytes for validation
    if isinstance(ns, str):
        try:
            ns_bytes = ns.encode('utf-8')
        except UnicodeEncodeError:
            errors.append(f"Namespace '{ns}' contains invalid UTF-8 characters.")
            return errors
    elif isinstance(ns, bytes):
        ns_bytes = ns
    else:
        errors.append(f"Namespace must be str or bytes, got: {type(ns).__name__}")
        return errors

    # Check for empty namespace
    if not ns_bytes or len(ns_bytes) == 0:
        errors.append("Namespace cannot be empty.")
    else:
        # Check for invalid characters (HBase doesn't allow all chars)
        # HBase allows: [a-zA-Z0-9_-]
        try:
            ns_str = ns_bytes.decode('utf-8') if isinstance(ns, bytes) else ns
            invalid_chars = set()
            for char in ns_str:
                if not (char.isalnum() or char in '_-'):
                    invalid_chars.add(char)
            if invalid_chars:
                errors.append(
                    f"Namespace contains invalid characters: {sorted(invalid_chars)}. "
                    "Only alphanumeric characters and underscore (_) are allowed."
                )
        except UnicodeDecodeError:
            errors.append(f"Namespace contains invalid UTF-8 sequence.")

    return errors


def validate_table_name(tb: any) -> List[str]:
    """Validate HBase table name.

    Args:
        tb: Table name value to validate (str or bytes)

    Returns:
        List of error messages. Empty list if valid.
    """
    errors: List[str] = []

    if tb is None:
        errors.append("Table name cannot be None.")
        return errors

    # Convert to bytes for validation
    if isinstance(tb, str):
        try:
            tb_bytes = tb.encode('utf-8')
        except UnicodeEncodeError:
            errors.append(f"Table name '{tb}' contains invalid UTF-8 characters.")
            return errors
    elif isinstance(tb, bytes):
        tb_bytes = tb
    else:
        errors.append(f"Table name must be str or bytes, got: {type(tb).__name__}")
        return errors

    # Check for empty table name
    if not tb_bytes or len(tb_bytes) == 0:
        errors.append("Table name cannot be empty.")
    else:
        # Check for invalid characters
        # HBase allows: [a-zA-Z0-9_.-]
        try:
            tb_str = tb_bytes.decode('utf-8') if isinstance(tb, bytes) else tb
            invalid_chars = set()
            for char in tb_str:
                if not (char.isalnum() or char in '_-.'):
                    invalid_chars.add(char)
            if invalid_chars:
                errors.append(
                    f"Table name contains invalid characters: {sorted(invalid_chars)}. "
                    "Only alphanumeric characters, underscore (_), and hyphen (-) are allowed."
                )
        except UnicodeDecodeError:
            errors.append(f"Table name contains invalid UTF-8 sequence.")

    # Check table name length
    if len(tb_bytes) > 65536:
        errors.append(
            f"Table name too long: {len(tb_bytes)} bytes. "
            "Maximum length is 65536 bytes."
        )

    return errors


def validate_row_key(rowkey: any) -> List[str]:
    """Validate HBase row key.

    Args:
        rowkey: Row key value to validate (str or bytes)

    Returns:
        List of error messages. Empty list if valid.
    """
    errors: List[str] = []

    if rowkey is None:
        errors.append("Row key cannot be None.")
        return errors

    # Convert to bytes for validation
    if isinstance(rowkey, str):
        try:
            rowkey_bytes = rowkey.encode('utf-8')
        except UnicodeEncodeError:
            errors.append(f"Row key '{rowkey}' contains invalid UTF-8 characters.")
            return errors
    elif isinstance(rowkey, bytes):
        rowkey_bytes = rowkey
    else:
        errors.append(f"Row key must be str or bytes, got: {type(rowkey).__name__}")
        return errors

    # Check for empty row key (empty row keys are allowed in HBase)
    # No validation needed for empty row keys

    # Check row key length
    if len(rowkey_bytes) > 2147483647:  # ~2GB in bytes
        errors.append(
            f"Row key too long: {len(rowkey_bytes)} bytes. "
            "Maximum length is ~2GB (2147483647 bytes)."
        )

    return errors


def validate_column_family(cf: any) -> List[str]:
    """Validate HBase column family name.

    Args:
        cf: Column family value to validate (str or bytes)

    Returns:
        List of error messages. Empty list if valid.
    """
    errors: List[str] = []

    if cf is None:
        errors.append("Column family cannot be None.")
        return errors

    # Convert to bytes for validation
    if isinstance(cf, str):
        try:
            cf_bytes = cf.encode('utf-8')
        except UnicodeEncodeError:
            errors.append(f"Column family '{cf}' contains invalid UTF-8 characters.")
            return errors
    elif isinstance(cf, bytes):
        cf_bytes = cf
    else:
        errors.append(f"Column family must be str or bytes, got: {type(cf).__name__}")
        return errors

    # Check for empty column family
    if not cf_bytes or len(cf_bytes) == 0:
        errors.append("Column family cannot be empty.")
    else:
        # Check for valid characters
        # HBase allows: [a-zA-Z0-9_-]
        try:
            cf_str = cf_bytes.decode('utf-8') if isinstance(cf, bytes) else cf
            invalid_chars = set()
            for char in cf_str:
                if not (char.isalnum() or char == '_'):
                    invalid_chars.add(char)
            if invalid_chars:
                errors.append(
                    f"Column family contains invalid characters: {sorted(invalid_chars)}. "
                    "Only alphanumeric characters and underscore (_) are allowed."
                )
        except UnicodeDecodeError:
            errors.append(f"Column family contains invalid UTF-8 sequence.")

    # Check column family length
    if len(cf_bytes) > 65536:
        errors.append(
            f"Column family too long: {len(cf_bytes)} bytes. "
            "Maximum length is 65536 bytes."
        )

    return errors
