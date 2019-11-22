from .consts import *
import struct
from io import BytesIO

class Util(object):

    @classmethod
    def parse_qword(cls, data, little_endian=True) -> int:
        x = '<' if little_endian else '>'
        v, = struct.unpack(x+'Q', data[:4])
        return v

    @classmethod
    def parse_dword(cls, data, little_endian=True) -> int:
        x = '<' if little_endian else '>'
        v, = struct.unpack(x+'I', data[:4])
        return v

    @classmethod
    def parse_word(cls, data, little_endian=True) -> int:
        x = '<' if little_endian else '>'
        v, = struct.unpack(x+'H', data[:2])
        return v

    @classmethod
    def parse_byte(cls, data) -> int:
        v, = struct.unpack('<B', data[:2])
        return v

    @classmethod
    def parse_qword_fo(cls, fileobj) -> int:
        return cls.parse_qword(fileobj.read(QWORD))

    @classmethod
    def parse_dword_fo(cls, fileobj) -> int:
        return cls.parse_dword(fileobj.read(DWORD))

    @classmethod
    def parse_word_fo(cls, fileobj) -> int:
        return cls.parse_word(fileobj.read(WORD))

    @classmethod
    def parse_byte_fo(cls, fileobj) -> int:
        return cls.parse_byte(fileobj.read(BYTE))

    @classmethod
    def read_unconstrained_null_terminated_string_data(cls, data, limit=4096):
        fileobj = BytesIO(data)
        return cls.read_unconstrained_null_terminated_string(fileobj, limit=limit)

    @classmethod
    def read_unconstrained_null_terminated_string(cls, fileobj, limit=4096):
        output = b''
        cnt = 0 # sanity check
        while limit > 0:
            c = fileobj.read(1)
            cnt += 1
            if c == b'\x00':
                break
            output = output + c
            limit += -1
        return cnt, output

    @classmethod
    def read_unconstrained_null_terminated_string_v(cls, meta_layer, offset=0, limit=4096):
        output = b''
        cnt = 0 # sanity check
        while limit > 0:
            c = meta_layer.read(offset+cnt, 1)
            cnt += 1
            if c == b'\x00':
                break
            limit += -1
            output = output + c
        return cnt, output

    @classmethod
    def decode(cls, data: bytes) -> (int, int, int):
        consumed = 2
        if len(data) < 2:
            raise Exception("Invalid number of bytes")
        a, b = struct.unpack("BB", data[:2])
        if a & 128 == 128 and len(data) > 2:
            a &= 127
            # bytes values return ints on direct access
            c = data[2]
            blop = (a << 16) | (b << 8) | c
            return 3, (blop & 0xFFF), (blop & 0xFFF000) >> 12
        elif a & 128 == 128:
            raise Exception("Invalid number of bytes, expected 3 bytes only got {}".format(len(data)))
        else:
            return 2, b, a