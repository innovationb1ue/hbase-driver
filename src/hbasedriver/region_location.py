from hbasedriver.region_info import RegionInfo
from hbasedriver.server_name import ServerName


class RegionLocation:
    def __init__(self, region_info: RegionInfo, server_name: ServerName, seq_num=-1):
        self.region_info = region_info
        self.server_name = server_name
        self.seq_num = seq_num
