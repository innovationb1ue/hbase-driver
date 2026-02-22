from collections import defaultdict

from hbasedriver.client.cluster_connection import ClusterConnection
import logging
logger = logging.getLogger('pybase.' + __name__)
from hbasedriver.model import Row
from hbasedriver.operations import Scan
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest
from hbasedriver.protobuf_py.Client_pb2 import Column
from hbasedriver.protobuf_py.Filter_pb2 import Filter
from hbasedriver.region import Region
from hbasedriver.regionserver import RsConnection


# ref to ClientScanner.java
class ResultScanner:
    """Scanner for iterating through results of a scan operation.

    ResultScanner supports two construction styles for backward compatibility:
    1) ResultScanner(scan, table_name, cluster_connection) - table-level scanner that can span regions
    2) ResultScanner(scanner_id, scan, rs_conn) - low-level regionserver-backed scanner (single-region)

    The scanner automatically handles region boundaries and can span multiple regions.
    Iteration returns Row objects (one per iteration).

    Example:
        >>> scan = Scan().add_column(b"cf", b"col")
        >>> with table.scan(scan) as scanner:
        ...     for row in scanner:
        ...         print(row.rowkey)
        ...         print(row.get(b"cf", b"col"))

    Attributes:
        scan: The Scan operation being executed
        closed: Whether the scanner has been closed
    """

    def __init__(self, a, b, c):
        # flexible constructor
        self.scanner_id = None
        self.cluster_cnn: 'ClusterConnection | None' = None
        self.table_name = None
        self.scan: 'Scan' = None
        self.rs_conn = None
        self.rl: 'Region | None' = None

        # state
        self.cache: list[Row] = []
        self.closed = False
        self.partial_buffer: list = []  # buffered cells for partial rows
        self.last_emitted_row = None

        # detect signature
        # old signature: (scanner_id: int, scan: Scan, rs_conn)
        if isinstance(a, int):
            self.scanner_id = a
            self.scan = b
            self.rs_conn = c
            # cluster connection not available in this mode; region-crossing unsupported
            return

        # new signature: (scan: Scan, table_name, cluster_connection)
        self.scan = a
        self.table_name = b
        self.cluster_cnn: 'ClusterConnection' = c

        # locate initial region
        if self.cluster_cnn is not None:
            self.rl = self.cluster_cnn.locate_region(self.table_name, self.scan.start_row)
            # establish a regionserver connection for fetching scanner results
            # Try to use connection pool if available
            host = self.rl.host.decode('utf-8') if isinstance(self.rl.host, bytes) else self.rl.host
            port = int(self.rl.port.decode('utf-8')) if isinstance(self.rl.port, bytes) else int(self.rl.port)

            # Try cluster connection pool first (for RS connections)
            if hasattr(self.cluster_cnn, '_conn_pool'):
                # Note: cluster_cnn._conn_pool is for meta connections, but we can use it
                # for RS connections as well if we configure it appropriately
                # For now, we'll use the factory to create RsConnection instances
                self.rs_conn: 'RsConnection' = self._get_rs_connection(host, port)
            else:
                try:
                    self.rs_conn: 'RsConnection' = RsConnection().connect(host, port)
                except Exception:
                    # fallback to meta region host if connecting to the reported host fails
                    try:
                        meta_loc = self.cluster_cnn.registry.get_meta_region_locations().locations[0].server_name
                        meta_host = meta_loc.host
                        meta_port = meta_loc.port
                        meta_host = meta_host.decode('utf-8') if isinstance(meta_host, bytes) else meta_host
                        meta_port = int(meta_port.decode('utf-8')) if isinstance(meta_port, bytes) else int(meta_port)
                        self.rs_conn = RsConnection().connect(meta_host, meta_port)
                    except Exception:
                        raise

    def __iter__(self):
        """Return the iterator (self).

        Returns:
            Self as iterator
        """
        return self

    def _row_from_cells(self, cells):
        """Build a Row object from an iterable of Cell protos."""
        if not cells:
            return None
        kv = defaultdict(dict)
        for c in cells:
            kv[c.family][c.qualifier] = c.value
        rowkey = cells[0].row
        return Row(rowkey, kv)

    def __next__(self):
        """Get the next row in the scan result.

        Returns:
            The next Row object

        Raises:
            StopIteration: If no more rows are available
        """
        if self.closed:
            raise StopIteration
        # Use next_batch to fetch a single Row for Python iteration semantics
        rows = self.next_batch(1)
        if not rows:
            raise StopIteration
        return rows[0]

    def close(self):
        """Close the scanner and release server resources.

        This should be called when the scanner is no longer needed to
        properly clean up the server-side scanner resource.

        Example:
            >>> scanner = table.scan(scan)
            >>> try:
            ...     for row in scanner:
            ...         print(row)
            ... finally:
            ...     scanner.close()
        """
        if self.closed:
            return

    def _get_rs_connection(self, host: str, port: int) -> 'RsConnection':
        """Get a connection to the region server.

        Tries to use connection pool if available, otherwise creates a new connection.

        Args:
            host: RegionServer host
            port: RegionServer port

        Returns:
            An RsConnection to the region server
        """
        # Try cluster connection pool if available
        if self.cluster_cnn and hasattr(self.cluster_cnn, '_conn_pool'):
            pool = self.cluster_cnn._conn_pool
            # Temporarily update the factory to create RsConnection instances
            original_factory = pool.connection_factory
            pool.connection_factory = lambda h, p: RsConnection().connect(h, p)
            try:
                return pool.get_connection(host, port)
            finally:
                pool.connection_factory = original_factory

        # Fallback to direct connection
        return RsConnection().connect(host, port)

    def next(self, n=None):
        """Compatibility helper: next() returns a single Row (like Java), next(n) returns up to n Rows."""
        if n is None:
            return self.__next__()
        else:
            return self.next_batch(n)

    def close(self):
        """Close the underlying server scanner if open and mark this scanner closed."""
        if self.closed:
            return
        try:
            if self.scanner_id and self.rs_conn:
                rq = ScanRequest()
                rq.scanner_id = self.scanner_id
                rq.close_scanner = True
                # best-effort close
                self.rs_conn.send_request(rq, 'Scan')
        except Exception:
            pass

        # Return connection to pool if using pooled connection
        if self.rs_conn and self.cluster_cnn and hasattr(self.cluster_cnn, '_conn_pool'):
            # For pooled connections, we return them to the pool
            # Note: We don't close the actual connection here - the pool manages it
            # However, if we created a new connection via RsConnection().connect(),
            # we should still close the scanner on the server side (done above)
            pass

        self.closed = True

    def next_batch(self, n: int):
        """Fetch up to n rows using server-side pagination where possible."""
        if self.closed:
            raise StopIteration
        if n <= 0:
            return []

        requested = n
        out_rows = []

        while len(out_rows) < requested:
            opened_here = False
            if self.scanner_id is None:
                opened_here = True
                if not self.rl:
                    raise StopIteration
                rq = ScanRequest()
                rq.region.type = 1
                rq.region.value = self.rl.region_encoded
                rq.scan.CopyFrom(self.scan.to_protobuf())
                rq.number_of_rows = requested - len(out_rows)
                rq.renew = True
                rq.client_handles_partials = True
                resp = self.rs_conn.send_request(rq, 'Scan')
                self.scanner_id = resp.scanner_id
            else:
                rq = ScanRequest()
                rq.scanner_id = self.scanner_id
                rq.number_of_rows = requested - len(out_rows)
                rq.client_handles_partials = True
                resp = self.rs_conn.send_request(rq, 'Scan')

            results = list(resp.results) if hasattr(resp, 'results') else []

            for res in results:
                if len(res.cell) == 0:
                    continue
                if getattr(res, 'partial', False):
                    self.partial_buffer.extend(list(res.cell))
                    continue
                else:
                    if self.partial_buffer:
                        combined = self.partial_buffer + list(res.cell)
                        row = self._row_from_cells(combined)
                        self.partial_buffer = []
                    else:
                        row = self._row_from_cells(list(res.cell))

                    if row is not None:
                        out_rows.append(row)
                        self.last_emitted_row = row

                    if len(out_rows) >= requested:
                        break

            more_in_region = getattr(resp, 'more_results_in_region', False)
            more_global = getattr(resp, 'more_results', False)

            # If this scanner was opened in this loop iteration and produced no rows,
            # try a follow-up scan request (some servers return an open-scanner response
            # without rows and expect clients to call again).
            if opened_here and not out_rows:
                continue

            if not more_in_region and more_global and self.cluster_cnn is not None:
                try:
                    close_rq = ScanRequest()
                    close_rq.scanner_id = self.scanner_id
                    close_rq.close_scanner = True
                    self.rs_conn.send_request(close_rq, 'Scan')
                except Exception:
                    pass

                if not self.last_emitted_row:
                    next_start = self.scan.start_row
                else:
                    next_start = self.last_emitted_row.rowkey + b'\x00'

                try:
                    next_rl = self.cluster_cnn.locate_region(self.table_name, next_start)
                except Exception:
                    break

                # If we locate the same region, there are no more regions to scan
                if next_rl.region_encoded == self.rl.region_encoded:
                    break

                self.rl = next_rl
                # Update the scan's start row so the new scanner opens after the last emitted row
                self.scan.with_start_row(next_start, inclusive=True)

                host = self.rl.host.decode('utf-8') if isinstance(self.rl.host, bytes) else self.rl.host
                port = int(self.rl.port.decode('utf-8')) if isinstance(self.rl.port, bytes) else int(self.rl.port)
                try:
                    self.rs_conn = self._get_rs_connection(host, port)
                except Exception:
                    # fallback to meta host when region host connection fails
                    try:
                        meta_loc = self.cluster_cnn.registry.get_meta_region_locations().locations[0].server_name
                        meta_host = meta_loc.host
                        meta_port = meta_loc.port
                        meta_host = meta_host.decode('utf-8') if isinstance(meta_host, bytes) else meta_host
                        meta_port = int(meta_port.decode('utf-8')) if isinstance(meta_port, bytes) else int(meta_port)
                        self.rs_conn = self._get_rs_connection(meta_host, meta_port)
                    except Exception:
                        raise
                self.scanner_id = None
                continue

            if not out_rows:
                break
            return out_rows

        return out_rows

    def __enter__(self):
        """Enter the context manager.

        Returns:
            The ResultScanner instance
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and close the scanner.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred

        Returns:
            False - don't suppress any exceptions
        """
        self.close()
        return False

# /**
# * This class has the logic for handling scanners for regions with and without replicas. 1. A scan
# * is attempted on the default (primary) region, or a specific region. 2. The scanner sends all the
# * RPCs to the default/specific region until it is done, or, there is a timeout on the
# * default/specific region (a timeout of zero is disallowed). 3. If there is a timeout in (2) above,
# * scanner(s) is opened on the non-default replica(s) only for Consistency.TIMELINE without specific
# * replica id specified. 4. The results from the first successful scanner are taken, and it is
# * stored which server returned the results. 5. The next RPCs are done on the above stored server
# * until it is done or there is a timeout, in which case, the other replicas are queried (as in (3)
# * above).
# */
