from src.regionserver import RsConnection

host = "127.0.0.1"
port = 16020


def test_locate_region():
    client = RsConnection()
    client.connect(host, port)
    client.locate_region()
