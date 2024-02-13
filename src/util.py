import functools


def to_bytes(arr):
    if len(arr) == 0:
        return []
    if type(arr[0]) == bytes:
        return arr
    res = []
    for i in arr:
        res.append(bytes(i, 'utf-8'))
    return res
