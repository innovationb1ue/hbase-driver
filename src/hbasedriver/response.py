from hbasedriver.protobuf_py.Client_pb2 import GetResponse, MutateResponse, ScanResponse
from hbasedriver.protobuf_py.Master_pb2 import (
    GetTableDescriptorsResponse,
    ListNamespacesResponse,
    SnapshotResponse,
    GetCompletedSnapshotsResponse,
    RestoreSnapshotResponse,
    IsSnapshotDoneResponse,
    GetClusterStatusResponse,
    BalanceResponse,
    SetBalancerRunningResponse,
    IsBalancerEnabledResponse,
)

response_types = {
    # master responses
    "GetTableDescriptors": GetTableDescriptorsResponse,
    "ListNamespaces": ListNamespacesResponse,
    # snapshot responses
    "Snapshot": SnapshotResponse,
    "GetCompletedSnapshots": GetCompletedSnapshotsResponse,
    "RestoreSnapshot": RestoreSnapshotResponse,
    "IsSnapshotDone": IsSnapshotDoneResponse,
    # cluster responses
    "GetClusterStatus": GetClusterStatusResponse,
    "Balance": BalanceResponse,
    "SetBalancerRunning": SetBalancerRunningResponse,
    "IsBalancerEnabled": IsBalancerEnabledResponse,
    # client responses
    "Get": GetResponse,
    "Mutate": MutateResponse,
    "Scan": ScanResponse,
}
