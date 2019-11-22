
from .header import MemscrimperHeader
from typing import Any, Dict
from .layer import MemscrimperBody

from io import BytesIO, BufferedReader


class Memscrimper(object):

    def __init__(self, src_fileobj=None, src_filename=None, load_header_only=False, ref_filename=None, ref_bytes=None, load=False,
                 load_ref_data=True, disable_debug=True):

        if disable_debug:
            MemscrimperHeader.disable_debug()
            MemscrimperBody.disable_debug()
        else:
            MemscrimperHeader.enable_debug()
            MemscrimperBody.enable_debug()

        self.msh = MemscrimperHeader(fileobj=src_fileobj, filename=src_filename)
        self.msb = None

        self.ref_filename = None
        self.ref_bytes = None
        self.ref_loaded = False
        self.ref_data_loaded = False
        self.ms_loaded = False

        if load:
            self.load(load_header_only)
            if ref_filename is not None or ref_bytes is not None:
                self.associate_reference_data(ref_filename,ref_bytes,load_from_file=load_ref_data)


    def load(self, load_header_only=False):
        self.msh.load()
        if load_header_only:
            self.msb = MemscrimperBody(self.method, self.page_size,
                                   self.body_bytes,
                                   self.uncompressed_size)
        else:
            self.msb = MemscrimperBody(self.method, self.page_size,
                                   self.body_bytes,
                                   self.uncompressed_size).load()
            self.ms_loaded = True

    def read_page(self, page_num=None, offset=None):
        if page_num is None and offset is None:
            return None
        if page_num is None:
            page_num = int((offset % self.msb.page_size) / self.msb.page_size)
        return self.msb.read_page_num(page_num)

    def read_to_target(self, target_filename=None, target_fileobj=None, use_buffer=True):
        return self.msb.read_to_target(target_filename=target_filename,
                                       target_fileobj=target_fileobj, use_buffer=use_buffer)

    def associate_reference_data(self, filename=None, data_bytes=None, load_from_file=False):
        if self.ref_loaded:
            raise Exception("Attempting to asssociate more than one reference")

        self.ref_loaded = True
        self.ref_filename = filename
        self.ref_bytes = data_bytes
        self.ref_data_loaded = load_from_file

        if self.ref_loaded and data_bytes is None:
            self.ref_bytes = open(self.ref_filename, 'rb').read()
            self.ref_fileobj = BytesIO(self.ref_bytes)

        elif self.ref_loaded:
            self.ref_fileobj = BytesIO(self.ref_bytes)
        else:
            self.ref_fileobj = open(self.ref_filename, 'rb')

        if self.ms_loaded:
            self.msb.associate_reference_file(self.ref_filename, self.ref_bytes, False)

    def where(self, page_num=None, offset=None) -> (str, object):
        if self.ms_loaded:
            return self.msb.where(page_num, offset)
        return (None, None)

    def collect_page_nums(self) -> dict:
        if self.ms_loaded:
            return self.msb.collect_page_nums()
        return None

    def audit_decompression(self, act_target_filename=None, act_target_fileobj=None, return_buffer=False,
                            page_num=None, fail_fast=True, debug=True):
        return self.msb.audit_decompression(act_target_filename=act_target_filename,
                                            act_target_fileobj=act_target_fileobj,
                                            return_buffer=return_buffer, page_num=page_num,
                                            fail_fast=fail_fast, debug=debug)

    def enumerate_page_num(self, page_num) -> dict:
        return self.msb.enumerate_page(page_num)

    def destroy(self) -> None:
        """Closes the file handle."""
        if self.ref_fileobj is not None:
            self.ref_fileobj.close()
        self.msh.destroy()
        self.msb.destroy()

    def __getstate__(self) -> Dict[str, Any]:

        if isinstance(self.ref_fileobj, BufferedReader):
            self.ref_fileobj = None

        results = self.__dict__
        results['msb'] = self.msb.__getstate__()
        results['msh'] = self.msh.__getstate__()
        return self.__dict__

    @property
    def minimum_address(self) -> int:
        return 0

    @property
    def maximum_address(self) -> int:
        return self.msb.maximum_address

    @property
    def maximum_page(self) -> int:
        return self.msb.maximum_page

    def write(self, address: int, data: bytes):
        """Writes the data from to the buffer."""
        raise Exception("Not supported")

    def destroy(self) -> None:
        """Closes the file handle."""
        self.ref_fileobj.close()

    def __getstate__(self) -> Dict[str, Any]:
        """Do not store the open _file_ attribute, our property will ensure the
        file is open when needed.

        This is necessary for multi-processing
        """
        self.ref_fileobj = None
        return self.__dict__

    @property
    def changed_pages(self):
        return self.msb.changed_pages

    def vol_read(self, address: int, length: int, pad: bool = False, source: bool = True, force_reload: bool = False) -> bytes:
        return self.msb.vol_read(address, length, pad, source, force_reload=force_reload)

    def read(self, page_num=None, offset=None, force_reload=False):
        return self.msb.read(page_num, offset, force_reload)

    def is_valid(self, offset: int, length: int) -> bool:
        return self.msb.is_valid(offset, length)

    def read_meta_page_num(self, pagenr):
        return self.msb.read_meta_page_num(pagenr)

    @property
    def reference(self):
        if self.msb is not None:
            return self.msb.reference_image_name
        return None

    @property
    def uncompressed_size(self):
        if self.msh is None:
            return None
        return self.msh.uncompressed_size

    @property
    def body_bytes(self):
        if self.msh is None:
            return None
        return self.msh.body_bytes

    @property
    def method(self):
        if self.msh is None:
            return None
        return self.msh.method

    @property
    def source(self):
        if self.msh is not None and self.msh.filename is not None:
            return self.msh.filename
        else:
            return "UNKNOWN"

    @property
    def page_size(self):
        return self.msh.page_size

    def read_from_reference(self, page_num=None, offset=None, force_reload=False):
        return self.msb.read_from_reference(page_num=page_num, offset=offset, force_reload=force_reload)

    def reset_pages(self):
        return self.msb.reset_pages()