from struct import unpack

from Client_pb2 import ScanResponse
from src.regionserver import RsConnection
from src.zk import locate_meta

host = "127.0.0.1"
port = 16020


def test_locate_region():
    host, port = locate_meta(["127.0.0.1"])
    rs = RsConnection()
    rs.connect(host, port)

    resp: ScanResponse = rs.locate_region("", "test_table", "000")
    if resp.scanner_id != 0:
        print("receive scanner id ", resp.scanner_id)
    assert len(resp.results) != 0
