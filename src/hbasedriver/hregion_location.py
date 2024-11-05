from hbasedriver.region_info import RegionInfo
from hbasedriver.server_name import ServerName


# /**
#  * Data structure to hold RegionInfo and the address for the hosting HRegionServer. Immutable.
#  * Comparable, but we compare the 'location' only: i.e. the hostname and port, and *not* the
#  * regioninfo. This means two instances are the same if they refer to the same 'location' (the same
#  * hostname and port), though they may be carrying different regions. On a big cluster, each client
#  * will have thousands of instances of this object, often 100 000 of them if not million. It's
#  * important to keep the object size as small as possible. <br>
#  * This interface has been marked InterfaceAudience.Public in 0.96 and 0.98.
#  */
class HRegionLocation:
    def __init__(self, region_info: RegionInfo, server_name: ServerName, seq_num=-1):
        self.region_info = region_info
        self.server_name = server_name
        self.seq_num = seq_num
