from .compression import Zip7, Gzip, Bzip2, NoInnerBase
from .util import Util
import struct
from typing import Any, Dict, IO, List, Optional, Union
from io import BufferedReader
import logging

mp_logger = logging.getLogger(__name__)


class MemscrimperHeader(object):
    header_structure = "<5s"
    MAJOR = "<H"
    MINOR = "<H"
    PAGESZ = "<I"
    UNCOMPRESSED = "<Q"
    MAGIC = b"MBCR\x00"
    DEBUG = False
    name = "MemscrimperHeader"
    # COMPRESSION_OPTIONS = [b'gzip', b'bzip2', b'zip7', b'noinner']
    OPTIONS = [b'interdedup', b'delta', b'noinner']

    SUPPORTED_COMPRESSION_CLS = {
        b'zip7': Zip7,
        b'gzip': Gzip,
        b'bzip2': Bzip2,
        b'noinner': NoInnerBase
    }

    @classmethod
    def disable_debug(cls):
        MemscrimperHeader.DEBUG = False

    @classmethod
    def enable_debug(cls):
        MemscrimperHeader.DEBUG = True

    @classmethod
    def log(cls, msg, how='debug'):
        if cls.DEBUG:
            mp_logger.debug("{} {}".format(cls.name, msg))

    def __init__(self, fileobj=None, filename=None):
        self.uncompressed_size = None
        self.page_size = None
        self.minor = None
        self.major = None
        self.method = None
        self.magic = None
        self.compression_cls = None
        self.body_bytes_offset = None
        self.offset = 0
        self.fileobj = fileobj
        self.filename = filename
        self.body_bytes = None
        self.body = None
        self.decompressed = None

        if fileobj is None and filename is None:
            raise Exception("Nothing provided")

        if fileobj is not None:
            self.fileobj = fileobj
            self.fileobj.seek(0)
        elif filename is not None:
            self.fileobj = open(self.filename, 'rb')

        # self._read_header()
        # self._load_body()

    def load(self) -> object:
        self._read_header()
        self._load_body()
        return self

    def _read_header(self) -> None:
        """Checks the vmware header to make sure it's valid."""
        # if "vmware" not in self._context.symbol_space:
        #     self._context.symbol_space.append(native.NativeTable("vmware", native.std_ctypes))

        # meta_layer = self.context.layers.get(self._meta_layer, None)
        header_size = struct.calcsize(self.header_structure)
        # data = meta_layer.read(0, header_size)
        data = self.fileobj.read(header_size)
        self.offset += header_size
        magic, = struct.unpack(self.header_structure, data)
        # if magic not in [self.MAGIC]:
        #     raise MemscrimperFormatException(self.name, "Wrong magic bytes for Vmware layer: {}".format(repr(magic)))
        if magic not in [self.MAGIC]:
            raise Exception(self.name, "Wrong magic bytes for {} layer: {}".format(self.name, repr(magic)))

        self._read_method()
        self._read_major()
        self._read_minor()
        self._read_page_size()
        self._read_uncompressed_size()
        self._set_compression()
        self.body_bytes_offset = self.offset

    def _read_method(self):
        method = b''
        cnt, self.method = Util.read_unconstrained_null_terminated_string(self.fileobj)
        # cnt, self.method = Util.read_unconstrained_null_terminated_string_v(self.meta_layer, self.offset)
        self.offset += cnt
        if len(self.method) == 0:
            raise Exception(self.name, "Method not provided for {}.".format(self.name))

    def _read_major(self):
        sunpak = self.MAJOR
        size = struct.calcsize(sunpak)
        data = self.fileobj.read(size)
        # data = self.meta_layer.read(0, size)
        v, = struct.unpack(sunpak, data)
        self.offset += size
        self.major = v

    def _read_minor(self):
        sunpak = self.MINOR
        size = struct.calcsize(sunpak)
        data = self.fileobj.read(size)
        # data = self.meta_layer.read(0, size)
        v, = struct.unpack(sunpak, data)
        self.offset += size
        self.minor = v

    def _read_page_size(self):
        sunpak = self.PAGESZ
        size = struct.calcsize(sunpak)
        data = self.fileobj.read(size)
        # data = self.meta_layer.read(0, size)
        v, = struct.unpack(sunpak, data)
        self.offset += size
        self.page_size = v

    def _read_uncompressed_size(self):
        sunpak = self.UNCOMPRESSED
        size = struct.calcsize(sunpak)
        data = self.fileobj.read(size)
        # data = self.meta_layer.read(0, size)
        v, = struct.unpack(sunpak, data)
        self.offset += size
        self.uncompressed_size = v


    def _set_compression(self):
        self.compression_cls = None
        for o, c in self.SUPPORTED_COMPRESSION_CLS.items():
            if o in self.method:
                self.compression_cls = c

        if self.compression_cls is None:
            self.compression_cls = self.SUPPORTED_COMPRESSION_CLS[b'noinner']

    def _load_body(self, decompress=True, load_bytes_only=True) -> bytes:
        self.fileobj.seek(self.body_bytes_offset)
        if decompress:
            self.body_bytes = self.compression_cls.decompress(self.fileobj, offset=0)
        else:
            self.body_bytes = self.fileobj.read()

        self.decompressed = decompress or self.compression_cls.NAME == NoInnerBase.NAME
        self.body = None
        # if not load_bytes_only:
        #     self.body = MemscrimperBody(self.method, self.body_bytes, self.uncompressed_size)
        return self.body_bytes
        

    def __getstate__(self) -> Dict[str, Any]:
        if isinstance(self.fileobj, BufferedReader):
            self.fileobj = None

        return self.__dict__

    def destroy(self) -> None:
        """Closes the file handle."""
        if self.fileobj is not None:
            self.fileobj.close()
