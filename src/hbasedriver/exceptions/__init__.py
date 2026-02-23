"""
Remote exceptions from HBase server.

These exceptions represent errors that occur on the HBase server
and are propagated back to the client.
"""

from hbasedriver.exceptions.RemoteException import RemoteException, TableExistsException

__all__ = ["RemoteException", "TableExistsException"]
