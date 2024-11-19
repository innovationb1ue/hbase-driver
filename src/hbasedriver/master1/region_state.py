from enum import Enum
from hbasedriver.protobuf_py.ClusterStatus_pb2 import RegionState as RegionStateProto


class RegionState:
    class State(Enum):
        OFFLINE = 1  # region is in an offline state
        OPENING = 2  # server has begun to open but not yet done
        OPEN = 3  # server opened region and updated meta
        CLOSING = 4  # server has begun to close but not yet done
        CLOSED = 5  # server closed region and updated meta
        SPLITTING = 6  # server started split of a region
        SPLIT = 7  # server completed split of a region
        FAILED_OPEN = 8  # failed to open, and won't retry any more
        FAILED_CLOSE = 9  # failed to close, and won't retry any more
        MERGING = 10  # server started merge a region
        MERGED = 11  # server completed merge a region
        SPLITTING_NEW = 12  # new region to be created when RS splits a parent
        # region but hasn't been created yet, or master doesn't
        # know it's already created
        MERGING_NEW = 13  # new region to be created when RS merges two
        # daughter regions but hasn't been created yet, or
        # master doesn't know it's already created
        ABNORMALLY_CLOSED = 14  # the region is CLOSED because of a RS crashes. Usually it is the same
        # with CLOSED, but for some operations such as merge/split, we can not
        # apply it to a region in this state, as it may lead to data loss as we
        # may have some data in recovered edits.

    # Mapping from RegionStateProto to State
    state_mapping = {
        RegionStateProto.State.OFFLINE: State.OFFLINE,
        RegionStateProto.State.OPENING: State.OPENING,
        RegionStateProto.State.OPEN: State.OPEN,
        RegionStateProto.State.CLOSING: State.CLOSING,
        RegionStateProto.State.CLOSED: State.CLOSED,
        RegionStateProto.State.SPLITTING: State.SPLITTING,
        RegionStateProto.State.SPLIT: State.SPLIT,
        RegionStateProto.State.FAILED_OPEN: State.FAILED_OPEN,
        RegionStateProto.State.FAILED_CLOSE: State.FAILED_CLOSE,
        RegionStateProto.State.MERGING: State.MERGING,
        RegionStateProto.State.MERGED: State.MERGED,
        RegionStateProto.State.SPLITTING_NEW: State.SPLITTING_NEW,
        RegionStateProto.State.MERGING_NEW: State.MERGING_NEW,
        RegionStateProto.State.ABNORMALLY_CLOSED: State.ABNORMALLY_CLOSED,
    }

    @staticmethod
    def convert(region_state: RegionStateProto.State) -> State:
        try:
            return RegionState.state_mapping[region_state]
        except KeyError:
            raise ValueError("Illegal state")
