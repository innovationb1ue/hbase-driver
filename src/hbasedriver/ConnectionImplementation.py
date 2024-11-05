from hbasedriver.user import User


class ConnectionImplementation:
    def __init__(self, conf: dict, user: User, executors=None):
        self.conf = conf
        self.user = user
        