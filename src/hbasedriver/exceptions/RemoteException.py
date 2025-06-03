class RemoteException(Exception):
    def __init__(self, msg=None, stack_trace=None):
        super(RemoteException, self).__init__(msg)
        self.msg = msg
        self.stack_trace = stack_trace

    def __str__(self):
        # Pretty format the message and optional stack trace
        parts = ["RemoteException occurred:"]
        if self.msg:
            parts.append(f"  Message     : {self.msg}")
        if self.stack_trace:
            parts.append("  Stack Trace :\n" + self._indent_trace(self.stack_trace))
        return "\n".join(parts)

    def _indent_trace(self, trace):
        # Indent each line for readability
        if isinstance(trace, str):
            return "\n".join("    " + line for line in trace.strip().splitlines())
        elif isinstance(trace, list):
            return "\n".join("    " + line for line in trace)
        return str(trace)


class TableExistsException(RemoteException):
    pass
