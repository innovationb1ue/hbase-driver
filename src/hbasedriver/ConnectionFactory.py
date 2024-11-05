from hbasedriver.ConnectionImplementation import ConnectionImplementation
from hbasedriver.user import User


class ConnectionFactory:
    def __init__(self):
        pass

    @staticmethod
    def create_connection(conf: dict, user=User()):
        zk_quorum = conf.get("hbase.zookeeper.quorum")
        return ConnectionImplementation(conf, user)
