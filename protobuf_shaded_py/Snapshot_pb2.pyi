import AccessControl_pb2 as _AccessControl_pb2
import FS_pb2 as _FS_pb2
import HBase_pb2 as _HBase_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SnapshotDescription(_message.Message):
    __slots__ = ("name", "table", "creation_time", "type", "version", "owner", "users_and_permissions", "ttl", "max_file_size")
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DISABLED: _ClassVar[SnapshotDescription.Type]
        FLUSH: _ClassVar[SnapshotDescription.Type]
        SKIPFLUSH: _ClassVar[SnapshotDescription.Type]
    DISABLED: SnapshotDescription.Type
    FLUSH: SnapshotDescription.Type
    SKIPFLUSH: SnapshotDescription.Type
    NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_FIELD_NUMBER: _ClassVar[int]
    CREATION_TIME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    USERS_AND_PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
    TTL_FIELD_NUMBER: _ClassVar[int]
    MAX_FILE_SIZE_FIELD_NUMBER: _ClassVar[int]
    name: str
    table: str
    creation_time: int
    type: SnapshotDescription.Type
    version: int
    owner: str
    users_and_permissions: _AccessControl_pb2.UsersAndPermissions
    ttl: int
    max_file_size: int
    def __init__(self, name: _Optional[str] = ..., table: _Optional[str] = ..., creation_time: _Optional[int] = ..., type: _Optional[_Union[SnapshotDescription.Type, str]] = ..., version: _Optional[int] = ..., owner: _Optional[str] = ..., users_and_permissions: _Optional[_Union[_AccessControl_pb2.UsersAndPermissions, _Mapping]] = ..., ttl: _Optional[int] = ..., max_file_size: _Optional[int] = ...) -> None: ...

class SnapshotFileInfo(_message.Message):
    __slots__ = ("type", "hfile", "wal_server", "wal_name")
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        HFILE: _ClassVar[SnapshotFileInfo.Type]
        WAL: _ClassVar[SnapshotFileInfo.Type]
    HFILE: SnapshotFileInfo.Type
    WAL: SnapshotFileInfo.Type
    TYPE_FIELD_NUMBER: _ClassVar[int]
    HFILE_FIELD_NUMBER: _ClassVar[int]
    WAL_SERVER_FIELD_NUMBER: _ClassVar[int]
    WAL_NAME_FIELD_NUMBER: _ClassVar[int]
    type: SnapshotFileInfo.Type
    hfile: str
    wal_server: str
    wal_name: str
    def __init__(self, type: _Optional[_Union[SnapshotFileInfo.Type, str]] = ..., hfile: _Optional[str] = ..., wal_server: _Optional[str] = ..., wal_name: _Optional[str] = ...) -> None: ...

class SnapshotRegionManifest(_message.Message):
    __slots__ = ("version", "region_info", "family_files")
    class StoreFile(_message.Message):
        __slots__ = ("name", "reference", "file_size")
        NAME_FIELD_NUMBER: _ClassVar[int]
        REFERENCE_FIELD_NUMBER: _ClassVar[int]
        FILE_SIZE_FIELD_NUMBER: _ClassVar[int]
        name: str
        reference: _FS_pb2.Reference
        file_size: int
        def __init__(self, name: _Optional[str] = ..., reference: _Optional[_Union[_FS_pb2.Reference, _Mapping]] = ..., file_size: _Optional[int] = ...) -> None: ...
    class FamilyFiles(_message.Message):
        __slots__ = ("family_name", "store_files")
        FAMILY_NAME_FIELD_NUMBER: _ClassVar[int]
        STORE_FILES_FIELD_NUMBER: _ClassVar[int]
        family_name: bytes
        store_files: _containers.RepeatedCompositeFieldContainer[SnapshotRegionManifest.StoreFile]
        def __init__(self, family_name: _Optional[bytes] = ..., store_files: _Optional[_Iterable[_Union[SnapshotRegionManifest.StoreFile, _Mapping]]] = ...) -> None: ...
    VERSION_FIELD_NUMBER: _ClassVar[int]
    REGION_INFO_FIELD_NUMBER: _ClassVar[int]
    FAMILY_FILES_FIELD_NUMBER: _ClassVar[int]
    version: int
    region_info: _HBase_pb2.RegionInfo
    family_files: _containers.RepeatedCompositeFieldContainer[SnapshotRegionManifest.FamilyFiles]
    def __init__(self, version: _Optional[int] = ..., region_info: _Optional[_Union[_HBase_pb2.RegionInfo, _Mapping]] = ..., family_files: _Optional[_Iterable[_Union[SnapshotRegionManifest.FamilyFiles, _Mapping]]] = ...) -> None: ...

class SnapshotDataManifest(_message.Message):
    __slots__ = ("table_schema", "region_manifests")
    TABLE_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    REGION_MANIFESTS_FIELD_NUMBER: _ClassVar[int]
    table_schema: _HBase_pb2.TableSchema
    region_manifests: _containers.RepeatedCompositeFieldContainer[SnapshotRegionManifest]
    def __init__(self, table_schema: _Optional[_Union[_HBase_pb2.TableSchema, _Mapping]] = ..., region_manifests: _Optional[_Iterable[_Union[SnapshotRegionManifest, _Mapping]]] = ...) -> None: ...
