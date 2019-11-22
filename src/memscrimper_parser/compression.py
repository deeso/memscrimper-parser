from io import BytesIO, IOBase
import gzip
import bz2
import enum

class Compression(enum.Enum):
    ZIP7 = 0
    GZIP = 1
    BZIP2 = 2
    NOINNER = 3

class NoInnerBase(object):
    NAME = 'NoInner'
    @classmethod
    def _decompress(cls, data: bytes) -> bytes:
        return data

    @classmethod
    def _compress(cls, data: bytes) -> bytes:
        return data

    @classmethod
    def get_fileobj(cls, data: bytes) -> BytesIO:
        fo = BytesIO(data)
        fo.seek(0)
        return fo

    @classmethod
    def compress_data(cls, data: bytes, offset: int=0, end: int=None) -> bytes:
        if end is None and offset < len(data):
            return cls._compress(data[offset:])
        elif offset + end < len(data):
            return cls._compress(data[offset:end])
        return None

    @classmethod
    def decompress_data(cls, data: bytes, offset: int=0, end: int=None) -> bytes:
        return cls.decompress(cls.get_fileobj(data), offset, end)

    @classmethod
    def decompress(cls, fileobj: IOBase, offset: int=0, end: int=None):
        decompressed_data = cls._decompress(fileobj)
        if end is None and offset < len(decompressed_data):
            return decompressed_data[offset:]
        elif offset + end < len(decompressed_data):
            return decompressed_data[offset:end]
        elif offset < len(decompressed_data):
            return decompressed_data[offset:]
        return None


class Gzip(NoInnerBase):
    NAME = 'Gzip'
    @classmethod
    def _compress(cls, data) -> bytes:
        fo = cls.get_fileobj(b'')
        cf = gzip.GzipFile(fileobj=fo, mode="wb", compresslevel=9)
        cf.write(data)
        cf.flush()
        cf.close()
        fo.seek(0)
        compressed_data = fo.read()
        return compressed_data

    @classmethod
    def _decompress(cls, fileobj) -> bytes:
        cf = gzip.GzipFile(fileobj=fileobj, mode="rb", compresslevel=9)
        decompressed_data = cf.read()
        return decompressed_data

class Bzip2(NoInnerBase):
    NAME = 'Bzip2'
    @classmethod
    def _compress(cls, data) -> bytes:
        fo = cls.get_fileobj(b'')
        cf = bz2.BZ2File(fo, mode="wb")
        cf.write(data)
        cf.flush()
        cf.close()
        fo.seek(0)
        compressed_data = fo.read()
        return compressed_data

    @classmethod
    def _decompress(cls, fileobj) -> bytes:
        cf = bz2.BZ2File(fileobj, mode="rb")
        decompressed_data = cf.read()
        return decompressed_data

class Zip7(NoInnerBase):
    NAME = 'Zip7'
    @classmethod
    def _compress(cls, data) -> bytes:
        raise Exception("Not supported")

    @classmethod
    def _decompress(cls, fileobj) -> bytes:
        raise Exception("Not supported")
