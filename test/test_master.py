from src.master import MasterConnection


def test_connect_master():
    host = "127.0.0.1"
    port = 16000
    client = MasterConnection()
    client.connect(host, port)
    b = client.conn.recv(4)
    print(b)
