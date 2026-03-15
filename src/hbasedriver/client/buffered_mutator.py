"""BufferedMutator for efficient bulk write operations.

This module provides BufferedMutator class for buffered/batched write support
with configurable buffer size, background flush, and exception handling.

Similar to Java HBase's BufferedMutator interface.
"""
from __future__ import annotations

import threading
from collections import defaultdict
from typing import TYPE_CHECKING, Optional, Union

from hbasedriver.operations.delete import Delete
from hbasedriver.operations.put import Put

if TYPE_CHECKING:
    from hbasedriver.client.table import Table

# Type alias for mutations
Mutation = Union[Put, Delete]


class BufferedMutatorParams:
    """Configuration parameters for BufferedMutator.

    Example:
        >>> params = BufferedMutatorParams()
        >>> params.write_buffer_size = 2 * 1024 * 1024  # 2MB
        >>> params.write_buffer_periodic_flush = 3000  # 3 seconds
    """

    def __init__(self):
        self.write_buffer_size: int = 2 * 1024 * 1024  # Default 2MB
        self.write_buffer_periodic_flush: int = 3000  # Default 3 seconds in ms
        self.max_key_value_size: int = -1  # No limit by default


class ExceptionListener:
    """Interface for exception listener callback.

    Implement this interface to handle exceptions that occur during
    background flush operations.
    """

    def on_exception(
        self, _mutation: Mutation, _exception: Exception
    ) -> bool:
        """Called when an exception occurs during mutation.

        Args:
            _mutation: The mutation that caused the exception
            _exception: The exception that was raised

        Returns:
            True to continue processing, False to abort
        """
        raise NotImplementedError


class BufferedMutator:
    """Buffered mutator for efficient bulk write operations.

    Provides buffered/batched write support with configurable buffer size,
    background flush thread, and exception handling. Similar to Java HBase's
    BufferedMutator interface.

    The BufferedMutator buffers mutations (Put/Delete) in memory and sends
    them to HBase in batches for better throughput. It supports:
    - Configurable buffer size (flushes when size limit reached)
    - Periodic background flush (configurable interval)
    - Explicit flush on demand
    - Auto-flush on close
    - Exception listener for handling failures

    Example:
        >>> # Using context manager (recommended)
        >>> with client.get_buffered_mutator(table_name) as mutator:
        ...     mutator.mutate(Put(b"row1").add_column(b"cf", b"col", b"value1"))
        ...     mutator.mutate(Put(b"row2").add_column(b"cf", b"col", b"value2"))
        ...     # Auto-flushes on close
        >>>
        >>> # Manual usage
        >>> mutator = client.get_buffered_mutator(table_name)
        >>> try:
        ...     mutator.mutate(put1)
        ...     mutator.mutate(put2)
        ...     mutator.flush()  # Explicitly flush
        ... finally:
        ...     mutator.close()

    Attributes:
        DEFAULT_WRITE_BUFFER_SIZE: Default buffer size (2MB)
        MIN_WRITE_BUFFER_SIZE: Minimum buffer size (64KB)
        DEFAULT_WRITE_PERIODIC_FLUSH: Default flush interval (3 seconds)
    """

    DEFAULT_WRITE_BUFFER_SIZE = 2 * 1024 * 1024  # 2MB
    MIN_WRITE_BUFFER_SIZE = 64 * 1024  # 64KB
    DEFAULT_WRITE_PERIODIC_FLUSH = 3000  # 3 seconds in milliseconds

    def __init__(
        self,
        table: 'Table',
        params: Optional[BufferedMutatorParams] = None
    ):
        """Initialize BufferedMutator.

        Args:
            table: The Table to write to
            params: Optional configuration parameters
        """
        self._table = table
        self._closed = False

        # Configuration
        if params is None:
            params = BufferedMutatorParams()

        self._write_buffer_size = max(
            params.write_buffer_size,
            self.MIN_WRITE_BUFFER_SIZE
        )
        self._periodic_flush_interval_ms = params.write_buffer_periodic_flush
        self._max_key_value_size = params.max_key_value_size

        # Internal buffer: grouped by rowkey to merge mutations
        # Format: {rowkey: list[Mutation]}
        self._mutations_buffer: dict[bytes, list[Mutation]] = defaultdict(list)
        self._current_buffer_size = 0

        # Exception listener
        self._exception_listener: Optional[ExceptionListener] = None

        # Thread safety
        self._lock = threading.Lock()

        # Background flush thread
        self._flush_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Track background exceptions
        self._background_exception: Optional[Exception] = None

        # Start background flush thread if interval > 0
        if self._periodic_flush_interval_ms > 0:
            self._start_flush_thread()

    def mutate(self, mutation: Mutation) -> None:
        """Buffer a single mutation.

        The mutation will be sent to HBase when the buffer is flushed
        (either explicitly, when buffer size is reached, or on close).

        Args:
            mutation: A Put or Delete operation

        Raises:
            RuntimeError: If BufferedMutator is closed
            ValueError: If mutation type is not supported
        """
        with self._lock:
            self._check_closed()
            self._validate_mutation(mutation)

            # Check size limit
            mutation_size = self._estimate_mutation_size(mutation)
            if (self._max_key_value_size > 0 and
                mutation_size > self._max_key_value_size):
                raise ValueError(
                    f"Mutation size {mutation_size} exceeds max key-value size "
                    f"{self._max_key_value_size}"
                )

            # Add to buffer
            rowkey = mutation.rowkey
            self._mutations_buffer[rowkey].append(mutation)
            self._current_buffer_size += mutation_size

            # Check if we need to flush
            if self._current_buffer_size >= self._write_buffer_size:
                self._flush_internal()

    def mutate_all(self, mutations: list[Mutation]) -> None:
        """Buffer multiple mutations.

        Args:
            mutations: List of Put or Delete operations

        Raises:
            RuntimeError: If BufferedMutator is closed
            ValueError: If any mutation type is not supported
        """
        for mutation in mutations:
            self.mutate(mutation)

    def flush(self) -> None:
        """Explicitly flush the buffer.

        Sends all buffered mutations to HBase. This method blocks until
        all mutations have been sent.

        Raises:
            RuntimeError: If BufferedMutator is closed
            Exception: If a background flush error occurred earlier
        """
        with self._lock:
            self._check_closed()
            self._check_background_exception()
            self._flush_internal()

    def close(self) -> None:
        """Close the BufferedMutator and flush remaining mutations.

        This method flushes any remaining buffered mutations and stops
        the background flush thread.

        Raises:
            Exception: If a background flush error occurred
        """
        with self._lock:
            if self._closed:
                return

            self._closed = True

            # Stop background flush thread
            self._stop_event.set()
            if self._flush_thread and self._flush_thread.is_alive():
                self._flush_thread.join(timeout=5)

            # Flush remaining mutations
            if self._mutations_buffer:
                self._flush_internal()

            # Check for any background exception
            self._check_background_exception()

    def get_write_buffer_size(self) -> int:
        """Get the current write buffer size in bytes.

        Returns:
            The write buffer size in bytes
        """
        return self._write_buffer_size

    def set_write_buffer_size(self, size: int) -> None:
        """Set the write buffer size.

        Args:
            size: The new buffer size in bytes

        Raises:
            ValueError: If size is below minimum
        """
        if size < self.MIN_WRITE_BUFFER_SIZE:
            raise ValueError(
                f"Buffer size {size} is below minimum {self.MIN_WRITE_BUFFER_SIZE}"
            )
        with self._lock:
            self._write_buffer_size = size

    def set_exception_listener(self, listener: Optional[ExceptionListener]) -> None:
        """Set the exception listener.

        The listener will be called when exceptions occur during
        background flush operations.

        Args:
            listener: An ExceptionListener implementation or None to remove
        """
        self._exception_listener = listener

    def get_exception_listener(self) -> Optional[ExceptionListener]:
        """Get the current exception listener.

        Returns:
            The current ExceptionListener or None
        """
        return self._exception_listener

    def get_name(self) -> bytes:
        """Get the table name.

        Returns:
            The table name as bytes
        """
        return self._table.tb

    def get_current_buffer_size(self) -> int:
        """Get the current size of the buffer in bytes.

        Returns:
            Current buffer size in bytes
        """
        with self._lock:
            return self._current_buffer_size

    def get_current_buffer_size_mutations(self) -> int:
        """Get the number of mutations in the buffer.

        Returns:
            Number of mutations currently buffered
        """
        with self._lock:
            return sum(
                len(mutations)
                for mutations in self._mutations_buffer.values()
            )

    def _start_flush_thread(self) -> None:
        """Start the background flush thread."""
        self._stop_event.clear()
        self._flush_thread = threading.Thread(
            target=self._periodic_flush_worker,
            daemon=True
        )
        self._flush_thread.start()

    def _periodic_flush_worker(self) -> None:
        """Background thread that periodically flushes the buffer."""
        interval_sec = self._periodic_flush_interval_ms / 1000.0

        while not self._stop_event.is_set():
            # Wait for interval or stop signal
            if self._stop_event.wait(timeout=interval_sec):
                break

            # Flush if buffer has data
            with self._lock:
                if self._mutations_buffer and not self._closed:
                    try:
                        self._flush_internal()
                    except Exception as e:
                        # Store exception for later reporting
                        self._background_exception = e
                        break

    def _flush_internal(self) -> None:
        """Internal flush method. Must be called with lock held."""
        if not self._mutations_buffer:
            return

        # Get all mutations to flush
        mutations_to_flush: list[Mutation] = []
        for _, mutations in self._mutations_buffer.items():
            mutations_to_flush.extend(mutations)

        # Clear buffer
        self._mutations_buffer.clear()
        self._current_buffer_size = 0

        # Execute mutations
        for mutation in mutations_to_flush:
            try:
                if isinstance(mutation, Put):
                    self._table.put(mutation)
                elif isinstance(mutation, Delete):
                    self._table.delete(mutation)
            except Exception as e:
                if self._exception_listener:
                    should_continue = self._exception_listener.on_exception(
                        mutation, e
                    )
                    if not should_continue:
                        raise
                else:
                    raise

    def _check_closed(self) -> None:
        """Check if BufferedMutator is closed.

        Raises:
            RuntimeError: If closed
        """
        if self._closed:
            raise RuntimeError("BufferedMutator is closed")

    def _check_background_exception(self) -> None:
        """Check for background exception and raise if present.

        Raises:
            Exception: The background exception if one occurred
        """
        if self._background_exception:
            exc = self._background_exception
            self._background_exception = None
            raise exc

    def _validate_mutation(self, mutation: Mutation) -> None:
        """Validate mutation type.

        Args:
            mutation: The mutation to validate

        Raises:
            ValueError: If mutation type is not supported
        """
        if not isinstance(mutation, (Put, Delete)):
            raise ValueError(
                f"Unsupported mutation type: {type(mutation).__name__}. "
                "Only Put and Delete are supported."
            )

    def _estimate_mutation_size(self, mutation: Mutation) -> int:
        """Estimate the size of a mutation in bytes.

        Args:
            mutation: The mutation to estimate

        Returns:
            Estimated size in bytes
        """
        size = len(mutation.rowkey)

        if isinstance(mutation, Put):
            for family, cells in mutation.family_cells.items():
                size += len(family)
                for cell in cells:
                    size += len(cell.qualifier) if cell.qualifier else 0
                    size += len(cell.value) if cell.value else 0
        elif isinstance(mutation, Delete):
            for family, cells in mutation.family_cells.items():
                size += len(family)
                for cell in cells:
                    size += len(cell.qualifier) if cell.qualifier else 0

        return size

    def __enter__(self) -> 'BufferedMutator':
        """Enter context manager.

        Returns:
            The BufferedMutator instance
        """
        return self

    def __exit__(self, exc_type, _exc_val, _exc_tb) -> bool:
        """Exit context manager and close.

        Args:
            exc_type: Exception type if any
            _exc_val: Exception value if any (unused)
            _exc_tb: Exception traceback if any (unused)

        Returns:
            False to not suppress exceptions
        """
        try:
            self.close()
        except Exception:
            # If there was already an exception, let it propagate
            if exc_type is not None:
                return False
            raise
        return False
