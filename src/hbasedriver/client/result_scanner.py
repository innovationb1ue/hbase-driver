from hbasedriver.client.cluster_connection import ClusterConnection
from hbasedriver.model import Row
from hbasedriver.operations import Scan
from hbasedriver.protobuf_py.Client_pb2 import ScanRequest
from hbasedriver.protobuf_py.Client_pb2 import Column
from hbasedriver.protobuf_py.Filter_pb2 import Filter
from hbasedriver.region import Region


# ref to ClientScanner.java
class ResultScanner:
    """ResultScanner supports two construction styles for backward compatibility:
    1) ResultScanner(scan, table_name, cluster_connection) - table-level scanner that can span regions
    2) ResultScanner(scanner_id, scan, rs_conn) - low-level regionserver-backed scanner (single-region)

    Iteration currently returns a list[Row] per next() call to preserve existing behavior.
    """

    def __init__(self, a, b, c):
        # flexible constructor
        self.scanner_id = None
        self.cluster_cnn: 'ClusterConnection' | None = None
        self.table_name = None
        self.scan: 'Scan' = None
        self.rs_conn = None
        self.rl: 'Region' | None = None

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
            from hbasedriver.regionserver import RsConnection
            host = self.rl.host.decode('utf-8') if isinstance(self.rl.host, bytes) else self.rl.host
            port = int(self.rl.port.decode('utf-8')) if isinstance(self.rl.port, bytes) else int(self.rl.port)
            self.rs_conn: 'RsConnection' = RsConnection().connect(host, port)

    def __iter__(self):
        return self

    def _row_from_cells(self, cells):
        """Build a Row object from an iterable of Cell protos."""
        if not cells:
            return None
        from collections import defaultdict
        kv = defaultdict(dict)
        for c in cells:
            kv[c.family][c.qualifier] = c.value
        rowkey = cells[0].row
        return Row(rowkey, kv)

    def __next__(self):
        if self.closed:
            raise StopIteration

        # how many rows to return in this iteration
        requested = getattr(self.scan, 'limit', 1) or 1
        out_rows = []

        while len(out_rows) < requested:
            # build or fetch results
            if self.scanner_id is None:
                # need a region to open scanner on
                if not self.rl:
                    # nothing to do in scanner-id mode without rs_conn
                    raise StopIteration

                rq = ScanRequest()
                rq.region.type = 1
                rq.region.value = self.rl.region_encoded
                rq.scan.CopyFrom(self.scan.to_protobuf())
                rq.number_of_rows = getattr(self.scan, 'caching', None) or requested
                rq.renew = True
                rq.client_handles_partials = True

                resp = self.rs_conn.send_request(rq, 'Scan')
                # record scanner id
                self.scanner_id = resp.scanner_id
            else:
                rq = ScanRequest()
                rq.scanner_id = self.scanner_id
                rq.number_of_rows = getattr(self.scan, 'caching', None) or requested
                rq.client_handles_partials = True
                resp = self.rs_conn.send_request(rq, 'Scan')

            # if no results returned
            results = list(resp.results) if hasattr(resp, 'results') else []

            # process results with partial assembly
            for res in results:
                # skip empty
                if len(res.cell) == 0:
                    continue

                if getattr(res, 'partial', False):
                    # buffer cells; do not emit until partial is False
                    self.partial_buffer.extend(list(res.cell))
                    continue
                else:
                    if self.partial_buffer:
                        # combine buffered partial fragments and current
                        combined = self.partial_buffer + list(res.cell)
                        row = self._row_from_cells(combined)
                        self.partial_buffer = []
                    else:
                        row = self._row_from_cells(list(res.cell))

                    if row is not None:
                        out_rows.append(row)
                        self.last_emitted_row = row

                    # stop early if we've satisfied requested
                    if len(out_rows) >= requested:
                        break

            # If we have enough rows, return them
            if len(out_rows) >= requested:
                return out_rows

            # If server indicated there are more results in other regions, move to next region.
            more_in_region = getattr(resp, 'more_results_in_region', False)
            more_global = getattr(resp, 'more_results', False)

            if not more_in_region and more_global and self.cluster_cnn is not None:
                # close existing scanner on this region if open
                try:
                    close_rq = ScanRequest()
                    close_rq.scanner_id = self.scanner_id
                    close_rq.close_scanner = True
                    self.rs_conn.send_request(close_rq, 'Scan')
                except Exception:
                    pass

                # determine start key for next region
                if not self.last_emitted_row:
                    # nothing emitted yet; use original scan.start_row
                    next_start = self.scan.start_row
                else:
                    # resume after last emitted row
                    next_start = self.last_emitted_row.rowkey + b'\x00'

                # locate next region
                try:
                    self.rl = self.cluster_cnn.locate_region(self.table_name, next_start)
                except Exception:
                    # if we can't locate next region, stop iteration
                    raise StopIteration

                # connect to new region server
                from hbasedriver.regionserver import RsConnection
                host = self.rl.host.decode('utf-8') if isinstance(self.rl.host, bytes) else self.rl.host
                port = int(self.rl.port.decode('utf-8')) if isinstance(self.rl.port, bytes) else int(self.rl.port)
                self.rs_conn = RsConnection().connect(host, port)
                # reset scanner id so we open a new scanner on new region in next loop
                self.scanner_id = None
                # continue loop to fetch more rows from new region
                continue

            # No more results globally or cannot continue; emit whatever we have or stop
            if out_rows:
                return out_rows
            raise StopIteration

    def load_cache(self):
        if self.closed:
            return

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
