from hbasedriver.exceptions.RemoteException import TableExistsException
from src.hbasedriver.master import MasterConnection

host = "127.0.0.1"
port = 16000


def test_connect_master():
    client = MasterConnection()
    client.connect(host, port)


def test_create_table():
    client = MasterConnection()
    client.connect(host, port)
    try:
        client.create_table("", "test_table", ["cf1", 'cf2'], split_keys=[b"111111", b"222222", b"333333"])
    except TableExistsException:
        pass


def test_delete_table():
    client = MasterConnection()
    client.connect(host, port)
    client.disable_table("", "test_table")
    client.delete_table("", "test_table")


def test_enable_table():
    client = MasterConnection()
    client.connect(host, port)
    client.enable_table("", "test_table")
