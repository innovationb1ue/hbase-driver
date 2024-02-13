from src.hbasedriver import zk


class Client:
    def __init__(self, zk_quorum: list):
        self.zk_quorum = zk_quorum
        zk.locate_meta(zk_quorum)
