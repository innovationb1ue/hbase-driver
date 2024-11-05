from hbasedriver.hregion_location import HRegionLocation


# /**
#  * Container for holding a list of {@link HRegionLocation}'s that correspond to the same range.

class RegionLocations:
    def __init__(self, locations: list[HRegionLocation]):
        self.locations = locations
