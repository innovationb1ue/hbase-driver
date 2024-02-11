from src.master import MasterConnection
from src.regionserver import RsConnection

host = "127.0.0.1"
port = 16000


def test_connect_master():
    client = MasterConnection()
    client.connect(host, port)


def test_create_table():
    client = MasterConnection()
    client.connect(host, port)
    client.create_table("", "test_table", ["cf1", 'cf2'])
