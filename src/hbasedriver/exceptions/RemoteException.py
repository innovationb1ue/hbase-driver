class RemoteException(BaseException):
    def __init__(self, msg=None, stack_trace=None):
        self.msg = msg
        self.stack_trace = stack_trace


class TableExistsException(RemoteException):
    pass
