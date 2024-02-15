import HBase_pb2 as _HBase_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Permission(_message.Message):
    __slots__ = ("type", "global_permission", "namespace_permission", "table_permission")
    class Action(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        READ: _ClassVar[Permission.Action]
        WRITE: _ClassVar[Permission.Action]
        EXEC: _ClassVar[Permission.Action]
        CREATE: _ClassVar[Permission.Action]
        ADMIN: _ClassVar[Permission.Action]
    READ: Permission.Action
    WRITE: Permission.Action
    EXEC: Permission.Action
    CREATE: Permission.Action
    ADMIN: Permission.Action
    class Type(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        Global: _ClassVar[Permission.Type]
        Namespace: _ClassVar[Permission.Type]
        Table: _ClassVar[Permission.Type]
    Global: Permission.Type
    Namespace: Permission.Type
    Table: Permission.Type
    TYPE_FIELD_NUMBER: _ClassVar[int]
    GLOBAL_PERMISSION_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_PERMISSION_FIELD_NUMBER: _ClassVar[int]
    TABLE_PERMISSION_FIELD_NUMBER: _ClassVar[int]
    type: Permission.Type
    global_permission: GlobalPermission
    namespace_permission: NamespacePermission
    table_permission: TablePermission
    def __init__(self, type: _Optional[_Union[Permission.Type, str]] = ..., global_permission: _Optional[_Union[GlobalPermission, _Mapping]] = ..., namespace_permission: _Optional[_Union[NamespacePermission, _Mapping]] = ..., table_permission: _Optional[_Union[TablePermission, _Mapping]] = ...) -> None: ...

class TablePermission(_message.Message):
    __slots__ = ("table_name", "family", "qualifier", "action")
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    FAMILY_FIELD_NUMBER: _ClassVar[int]
    QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    table_name: _HBase_pb2.TableName
    family: bytes
    qualifier: bytes
    action: _containers.RepeatedScalarFieldContainer[Permission.Action]
    def __init__(self, table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., family: _Optional[bytes] = ..., qualifier: _Optional[bytes] = ..., action: _Optional[_Iterable[_Union[Permission.Action, str]]] = ...) -> None: ...

class NamespacePermission(_message.Message):
    __slots__ = ("namespace_name", "action")
    NAMESPACE_NAME_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    namespace_name: bytes
    action: _containers.RepeatedScalarFieldContainer[Permission.Action]
    def __init__(self, namespace_name: _Optional[bytes] = ..., action: _Optional[_Iterable[_Union[Permission.Action, str]]] = ...) -> None: ...

class GlobalPermission(_message.Message):
    __slots__ = ("action",)
    ACTION_FIELD_NUMBER: _ClassVar[int]
    action: _containers.RepeatedScalarFieldContainer[Permission.Action]
    def __init__(self, action: _Optional[_Iterable[_Union[Permission.Action, str]]] = ...) -> None: ...

class UserPermission(_message.Message):
    __slots__ = ("user", "permission")
    USER_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_FIELD_NUMBER: _ClassVar[int]
    user: bytes
    permission: Permission
    def __init__(self, user: _Optional[bytes] = ..., permission: _Optional[_Union[Permission, _Mapping]] = ...) -> None: ...

class UsersAndPermissions(_message.Message):
    __slots__ = ("user_permissions",)
    class UserPermissions(_message.Message):
        __slots__ = ("user", "permissions")
        USER_FIELD_NUMBER: _ClassVar[int]
        PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
        user: bytes
        permissions: _containers.RepeatedCompositeFieldContainer[Permission]
        def __init__(self, user: _Optional[bytes] = ..., permissions: _Optional[_Iterable[_Union[Permission, _Mapping]]] = ...) -> None: ...
    USER_PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
    user_permissions: _containers.RepeatedCompositeFieldContainer[UsersAndPermissions.UserPermissions]
    def __init__(self, user_permissions: _Optional[_Iterable[_Union[UsersAndPermissions.UserPermissions, _Mapping]]] = ...) -> None: ...

class GrantRequest(_message.Message):
    __slots__ = ("user_permission", "merge_existing_permissions")
    USER_PERMISSION_FIELD_NUMBER: _ClassVar[int]
    MERGE_EXISTING_PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
    user_permission: UserPermission
    merge_existing_permissions: bool
    def __init__(self, user_permission: _Optional[_Union[UserPermission, _Mapping]] = ..., merge_existing_permissions: bool = ...) -> None: ...

class GrantResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class RevokeRequest(_message.Message):
    __slots__ = ("user_permission",)
    USER_PERMISSION_FIELD_NUMBER: _ClassVar[int]
    user_permission: UserPermission
    def __init__(self, user_permission: _Optional[_Union[UserPermission, _Mapping]] = ...) -> None: ...

class RevokeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetUserPermissionsRequest(_message.Message):
    __slots__ = ("type", "table_name", "namespace_name", "column_family", "column_qualifier", "user_name")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    NAMESPACE_NAME_FIELD_NUMBER: _ClassVar[int]
    COLUMN_FAMILY_FIELD_NUMBER: _ClassVar[int]
    COLUMN_QUALIFIER_FIELD_NUMBER: _ClassVar[int]
    USER_NAME_FIELD_NUMBER: _ClassVar[int]
    type: Permission.Type
    table_name: _HBase_pb2.TableName
    namespace_name: bytes
    column_family: bytes
    column_qualifier: bytes
    user_name: bytes
    def __init__(self, type: _Optional[_Union[Permission.Type, str]] = ..., table_name: _Optional[_Union[_HBase_pb2.TableName, _Mapping]] = ..., namespace_name: _Optional[bytes] = ..., column_family: _Optional[bytes] = ..., column_qualifier: _Optional[bytes] = ..., user_name: _Optional[bytes] = ...) -> None: ...

class GetUserPermissionsResponse(_message.Message):
    __slots__ = ("user_permission",)
    USER_PERMISSION_FIELD_NUMBER: _ClassVar[int]
    user_permission: _containers.RepeatedCompositeFieldContainer[UserPermission]
    def __init__(self, user_permission: _Optional[_Iterable[_Union[UserPermission, _Mapping]]] = ...) -> None: ...

class CheckPermissionsRequest(_message.Message):
    __slots__ = ("permission",)
    PERMISSION_FIELD_NUMBER: _ClassVar[int]
    permission: _containers.RepeatedCompositeFieldContainer[Permission]
    def __init__(self, permission: _Optional[_Iterable[_Union[Permission, _Mapping]]] = ...) -> None: ...

class CheckPermissionsResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HasUserPermissionsRequest(_message.Message):
    __slots__ = ("user_name", "permission")
    USER_NAME_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_FIELD_NUMBER: _ClassVar[int]
    user_name: bytes
    permission: _containers.RepeatedCompositeFieldContainer[Permission]
    def __init__(self, user_name: _Optional[bytes] = ..., permission: _Optional[_Iterable[_Union[Permission, _Mapping]]] = ...) -> None: ...

class HasUserPermissionsResponse(_message.Message):
    __slots__ = ("has_user_permission",)
    HAS_USER_PERMISSION_FIELD_NUMBER: _ClassVar[int]
    has_user_permission: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, has_user_permission: _Optional[_Iterable[bool]] = ...) -> None: ...
