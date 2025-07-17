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


def to_string_binary(data: bytes) -> str:
    result = []
    for b in data:
        # printable ASCII range: 32 (space) to 126 (~)
        if 32 <= b <= 126 and chr(b) not in ('\\', '"'):
            result.append(chr(b))
        else:
            result.append("\\x{:02x}".format(b))
    return ''.join(result)
