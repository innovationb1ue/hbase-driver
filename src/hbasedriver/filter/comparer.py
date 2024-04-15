class Comparer:
    INSTANCE = None

    def __init__(self):
        pass

    @staticmethod
    def get_instance():
        if Comparer.INSTANCE is None:
            Comparer.INSTANCE = Comparer()
        return Comparer.INSTANCE

    @staticmethod
    def compare_to(self, buf1: bytearray, o1: int, l1: int, buf2: bytearray, o2: int, l2: int) -> int:
        end1 = o1 + l1
        end2 = o2 + l2
        for i in range(o1, end1):
            if i >= len(buf1) or i >= end1:
                break
            a = buf1[i] & 0xFF
            if o2 >= len(buf2) or o2 >= end2:
                break
            b = buf2[o2] & 0xFF
            if a != b:
                return a - b
            o2 += 1
        return l1 - l2

    @staticmethod
    def compare_to_with_buffer(self, buf1: bytearray, o1: int, l1: int, buf2: bytearray, o2: int, l2: int) -> int:
        end1 = o1 + l1
        end2 = o2 + l2
        for i in range(o1, end1):
            if i >= len(buf1) or i >= end1:
                break
            a = buf1[i] & 0xFF
            if o2 >= len(buf2) or o2 >= end2:
                break
            b = buf2[o2] & 0xFF
            if a != b:
                return a - b
            o2 += 1
        return l1 - l2
