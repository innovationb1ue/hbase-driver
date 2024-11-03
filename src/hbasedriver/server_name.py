# uniquely indentify a hbase server in the cluster.


SERVERNAME_SEPARATOR_STR = ','
SERVERNAME_SEPARATOR = b','


class ServerName:
    def __init__(self, hostname: str, port: int, start_code: int):
        self.host = hostname
        self.port = port
        self.start_code = start_code

    @staticmethod
    def get_server_name(hostname: str, port: int, start_code: int):
        return hostname.lower() + SERVERNAME_SEPARATOR_STR + str(port) + SERVERNAME_SEPARATOR_STR + str(start_code)
