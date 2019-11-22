from hashlib import md5
from .sections import MemscrimperInterIntraDedupSection, MemscrimperDistinctSection, MemscrimperInterDedupSection, \
                      MemscrimperDiffSection
from typing import Any, Dict
from io import BytesIO, BufferedReader
from .util import Util
import logging

mp_logger = logging.getLogger(__name__)

class MemscrimperBody(object):
    name = 'MemscrimperBody'
    DEBUG = False
    DISTINCT_PAGES = "<I"
    INTERDEDUPNOINTRADELTA = b"interdedupnointradelta"
    INTERDEDUPNOINTRA = b"interdedupnointra"
    INTERDEDUPDELTA = b"interdedupdelta"
    INTERDEDUP = b"interdedup"

    @classmethod
    def disable_debug(cls):
        MemscrimperInterIntraDedupSection.DEBUG = False
        MemscrimperDistinctSection.DEBUG = False
        MemscrimperInterDedupSection.DEBUG = False
        MemscrimperDiffSection.DEBUG = False
        MemscrimperBody.DEBUG = False

    @classmethod
    def enable_debug(cls):
        MemscrimperInterIntraDedupSection.DEBUG = True
        MemscrimperDistinctSection.DEBUG = True
        MemscrimperInterDedupSection.DEBUG = True
        MemscrimperDiffSection.DEBUG = True
        MemscrimperBody.DEBUG = True

    @classmethod
    def log(cls, msg, how='debug'):
        if cls.DEBUG:
            mp_logger.debug("{} {}".format(cls.name, msg))

    def __init__(self, method, page_size, body_bytes, uncompressed_size):
        self.method = method
        self.body_bytes = body_bytes
        self.uncompressed_size = uncompressed_size
        self.start = 0
        self.offset = 0
        self.interval_list = None
        self.page_size = page_size

        self.page_data_base = None
        self.page_data_sz = None

        self.num_pagenrs = None

        self.distinct_pages_section_start = None
        self.distinct_pages_section_end = None
        self.distinct_pages_section = None

        self.interdedup_pages_section_start = None
        self.interdedup_pages_section_end = None
        self.interdedup_pages_section = None

        self.diff_pages_section_start = None
        self.diff_pages_section_end = None
        self.diff_pages_section = None

        self.interdedupnointra_pages_section_start = None
        self.interdedupnointra_pages_section_end = None
        self.interdedupnointra_pages_section = None

        self.src_fileobj = BytesIO(body_bytes)
        self.ref_fileobj = None

        self.pages = []
        self.interval_list = []

        self._body_parser = None
        self._body_parser_method = None

        self.reference_look_up = {}

        self.src_page_data = {}
        self.page_data = {}
        self.ref_file_obj = None
        self._changed_pages = set()
        # self.src_file_obj = BytesIO(self.body_bytes)

        self.methods_handlers = [
            (self.INTERDEDUPNOINTRADELTA, self.handle_interdedupnointradelta),
            (self.INTERDEDUPNOINTRA, self.handle_interdedupnointra),
            (self.INTERDEDUPDELTA, self.handle_interdedupdelta),
            (self.INTERDEDUP, self.handle_interdedup),
        ]

        for name, handler in self.methods_handlers:
            if self.method.find(name) > -1:
                self._body_parser_method = name
                self._body_parser = handler
                break

        if self._body_parser is None:
            raise Exception("{} is not a valid method".format(self.method))

    def associate_reference_file(self, filename=None, data_bytes=None, load_from_file=False):
        self.ref_filename = filename
        self.ref_bytes = data_bytes
        self.ref_loaded = filename is not None or data_bytes is not None
        if data_bytes is None and not load_from_file:
            self.ref_fileobj = open(self.ref_filename, 'rb')
        elif data_bytes is not None:
            self.ref_fileobj = BytesIO(self.ref_bytes)

        # if self.distinct_pages_section is not None:
        #     self.distinct_pages_section.associate_reference_file(filename=filename, data_bytes=data_bytes,
        #                                                          load_from_file=load_from_file)
        # if self.interdedup_pages_section is not None:
        #     self.interdedup_pages_section.associate_reference_file(filename=filename, data_bytes=data_bytes,
        #                                                            load_from_file=load_from_file)
        # if self.diff_pages_section is not None:
        #     self.diff_pages_section.associate_reference_file(filename=filename, data_bytes=data_bytes,
        #                                                      load_from_file=load_from_file)
        # if self.interdedupnointra_pages_section is not None:
        #     self.interdedupnointra_pages_section.associate_reference_file(filename=filename, data_bytes=data_bytes,
        #                                                                   load_from_file=load_from_file)

    def where(self, page_num=None, offset=None) -> (str, object):
        if page_num is None and offset is None:
            return None
        if offset is not None:
            page_num = (offset % self.page_size) / self.page_size

        has_page = False
        result = None
        name = None
        if not has_page is None and self.distinct_pages_section is not None:
            has_page = self.distinct_pages_section.has_page(page_num=page_num, offset=offset)
            if has_page:
                result = self.distinct_pages_section
                name = self.distinct_pages_section.name

        if has_page is None and self.interdedup_pages_section is not None:
            has_page = self.interdedup_pages_section.has_page(page_num=page_num, offset=offset)
            if has_page:
                result = self.interdedup_pages_section
                name = self.interdedup_pages_section.name

        if has_page is None and self.interdedupnointra_pages_section is not None:
            has_page = self.interdedupnointra_pages_section.has_page(page_num=page_num, offset=offset)
            if has_page:
                result = self.interdedupnointra_pages_section
                name = self.interdedupnointra_pages_section.name

        if has_page is None and self.diff_pages_section is not None:
            has_page = self.diff_pages_section.has_page(page_num=page_num, offset=offset)
            if has_page:
                result = self.diff_pages_section
                name = self.diff_pages_section.name

        return (name, result)

    def collect_page_nums(self) -> dict:
        results = {}

        if self.distinct_pages_section is not None:
            results[self.distinct_pages_section.name] = list(self.distinct_pages_section.reference_pages)

        if self.interdedup_pages_section is not None:
            results[self.interdedup_pages_section.name] = list(self.interdedup_pages_section.reference_pages)

        if self.interdedupnointra_pages_section is not None:
            results[self.interdedupnointra_pages_section.name] = list(self.interdedupnointra_pages_section.reference_pages)

        if self.diff_pages_section is not None:
            results[self.diff_pages_section.name] = list(self.diff_pages_section.reference_pages)

        return results

    @property
    def minimum_address(self) -> int:
        return 0

    @property
    def maximum_address(self) -> int:
        return self.uncompressed_size

    @property
    def maximum_page(self) -> int:
        return self.uncompressed_size / self.page_size

    def write(self, address: int, data: bytes):
        """Writes the data from to the buffer."""
        raise Exception("Not supported")

    def is_valid(self, offset: int, length: int = 1) -> bool:
        """Returns whether the offset is valid or not."""
        if length <= 0:
            raise ValueError("Length must be positive")
        return bool(self.minimum_address <= offset <= self.maximum_address
                    and self.minimum_address <= offset + length - 1 <= self.maximum_address)

    def destroy(self) -> None:
        """Closes the file handle."""
        self.ref_fileobj.close()

    def __getstate__(self) -> Dict[str, Any]:
        if isinstance(self.ref_fileobj, BufferedReader):
            self.ref_fileobj = None
        return self.__dict__

    @property
    def changed_pages(self):
        return self._changed_pages

    def is_valid(self, offset: int, length: int) -> bool:
        return bool(self.minimum_address <= offset <= self.maximum_address and \
                    self.minimum_address <= offset + length - 1 <= self.maximum_address)

    def enumerate_page_num(self, page_num) -> dict:
        results = {}

        if self.distinct_pages_section is not None:
            has_page = self.distinct_pages_section.has_page(page_num=page_num)
            if has_page:
                results[self.distinct_pages_section.name] = [page_num, self.distinct_pages_section]

        if self.interdedup_pages_section is not None:
            has_page = self.interdedup_pages_section .has_page(page_num=page_num)
            if has_page:
                results[self.interdedup_pages_section .name] = [page_num, self.interdedup_pages_section ]

        if self.interdedupnointra_pages_section is not None:
            has_page = self.interdedupnointra_pages_section.has_page(page_num=page_num)
            if has_page:
                results[self.interdedupnointra_pages_section.name] = [page_num, self.interdedupnointra_pages_section]


        if self.diff_pages_section is not None:
            has_page = self.diff_pages_section.has_page(page_num=page_num)
            if has_page:
                results[self.diff_pages_section.name] = [page_num, self.diff_pages_section]

        return results

    def reset_pages(self):
        self.page_data = {}

    def vol_read(self, address: int, length: int, pad: bool = False, source: bool = True, force_reload=False) -> bytes:

        if not self.is_valid(address, length):
            invalid_address = address
            if self.minimum_address < address <= self.maximum_address:
                invalid_address = self.maximum_address + 1
            raise Exception(self.name, invalid_address, "Offset outside of the buffer boundaries")
        min_page_num = int(address / self.page_size)
        max_page_num = int((address+length) / self.page_size)
        pages = []
        for page_num in range(min_page_num, max_page_num+1):
            page = None
            if source:
                page = self.read_page_num(page_num, force_reload=force_reload)
            else:
                page = self.read_from_reference(page_num, force_reload=force_reload)
            pages.append(page)

        data = b"".join(pages)
        start = address % self.page_size

        return data[ start : start + length]

    def read(self, offset: int, length: int, pad: bool) -> bytes:
        return self.vol_read(offset, length, pad)

    def load(self) -> object:
        self._read_reference_name()
        self._body_parser()
        return self

    def read_from_reference(self, page_num=None, offset=None, force_reload=False):
        if page_num is None and offset is None:
            return None
        if page_num is not None:
            offset = page_num * self.page_size
        if page_num is None:
            page_num = int((offset % self.page_size) / self.page_size)
        if page_num in self.page_data and not force_reload:
            return self.page_data[page_num]
        if self.ref_fileobj is None:
            return None
        self.ref_fileobj.seek(offset)
        page = self.ref_fileobj.read(self.page_size)
        self.page_data[page_num] = page
        return page

    def read_from_src(self, src_page_num=None, offset=None, force_reload=False):
        if src_page_num is None and offset is None:
            return None
        if src_page_num is not None:
            offset = src_page_num * self.page_size + self.page_data_base

        if src_page_num in self.src_page_data and force_reload:
            return self.src_page_data[src_page_num]

        src_offset = self.page_size * src_page_num + self.page_data_base
        self.src_fileobj.seek(src_offset)
        page = self.src_fileobj.read(self.page_size)
        self.src_page_data[src_page_num] = page
        return page

    def read_to_target(self, target_filename=None, target_fileobj=None, use_buffer=False):
        if use_buffer:
            target_fileobj = BytesIO()
        elif target_filename is not None and target_fileobj is None:
            target_fileobj = open(target_filename, 'wb')

        if target_fileobj is None:
            raise Exception("Nothing to write too.")

        num_pages = self.uncompressed_size / self.page_size
        pagenr = 0
        offset = 0
        while pagenr < num_pages:
            page = self.read_page_num(pagenr)

            if page is None:
                raise Exception("Unable to read from page number: {}".format(pagenr))

            if target_fileobj is not None:
                target_fileobj.write(page)

            offset += self.page_size
            pagenr += 1

        return target_fileobj

    def audit_decompression(self, act_target_filename=None, act_target_fileobj=None,
                            return_buffer=False, page_num=None, fail_fast=True, debug=False):

        def audit_page(pagenr, act_target, output):
            page = self.read_page_num(pagenr)
            act_page = act_target.read(self.page_size)
            offset = pagenr * self.page_size
            am = None
            sm = None
            if page is not None:
                sm = md5(page).hexdigest()
            if act_page is not None:
                am = md5(act_page).hexdigest()
            if output is not None and page is not None:
                output.write(page)
            if debug:
                self.log("pagenr: {} offset: {} md5 actual:{} == src:{} = {}".format(pagenr, offset, am, sm, am==sm))
            return (pagenr, offset, am, sm, am == sm)


        target_fileobj = None
        if return_buffer:
            target_fileobj = BytesIO()

        if act_target_filename is not None and act_target_fileobj is None:
            act_target_fileobj = open(act_target_filename, 'rb')

        if act_target_fileobj is None:
            raise Exception("Unable to audit the file")

        if page_num is not None:
            return target_fileobj, [audit_page(page_num, act_target_fileobj, target_fileobj)]

        results = []
        act_target_fileobj.seek(0)
        pagenr = 0
        num_pages = self.uncompressed_size / self.page_size
        while pagenr < num_pages:
            r = audit_page(pagenr, act_target_fileobj, target_fileobj)
            results.append(r)
            if fail_fast and r[-1] == False:
                break
            pagenr += 1
        return target_fileobj, results

    def read_meta_page_num(self, pagenr):
        page = None
        if self.distinct_pages_section is not None and \
                self.distinct_pages_section.has_page(pagenr):
            pass
        elif self.diff_pages_section is not None and \
                self.diff_pages_section.has_page(pagenr):

            page = b'\x00' * self.page_size
            page = self.diff_pages_section.apply_page_diff(pagenr, page)
        elif self.interdedupnointra_pages_section is not None and \
                self.interdedupnointra_pages_section.has_page(pagenr):

            page_num = self.interdedupnointra_pages_section.convert_pagenr(pagenr)
            page = self.read_from_src(src_page_num=page_num)
        elif self.interdedup_pages_section is not None and \
                self.interdedup_pages_section.has_page(pagenr):
            page_num = self.interdedup_pages_section.convert_pagenr(pagenr)
            page = self.read_from_src(src_page_num=page_num)
        return page

    def read_page_num(self, pagenr, force_reload=False):
        page = None
        if self.distinct_pages_section is not None and \
                self.distinct_pages_section.has_page(pagenr):

            page_num = self.distinct_pages_section.convert_pagenr(pagenr)
            page = self.read_from_reference(page_num=page_num, force_reload=force_reload)
        elif self.diff_pages_section is not None and \
                self.diff_pages_section.has_page(pagenr):

            page_num = self.diff_pages_section.convert_pagenr(pagenr)
            page = self.read_from_reference(page_num=page_num, force_reload=force_reload)
            page = self.diff_pages_section.apply_page_diff(pagenr, page)
        elif self.interdedupnointra_pages_section is not None and \
                self.interdedupnointra_pages_section.has_page(pagenr):

            page_num = self.interdedupnointra_pages_section.convert_pagenr(pagenr)
            page = self.read_from_src(src_page_num=page_num, force_reload=force_reload)
        elif self.interdedup_pages_section is not None and \
                self.interdedup_pages_section.has_page(pagenr):
            page_num = self.interdedup_pages_section.convert_pagenr(pagenr)
            page = self.read_from_src(src_page_num=page_num, force_reload=force_reload)
        else:
            page = self.read_from_reference(page_num=pagenr, force_reload=force_reload)
        return page

    def _did_page_change(self, pagenr) -> bool:
        changed = False
        if self.distinct_pages_section is not None and \
                self.distinct_pages_section.has_page(pagenr):
            return False
        elif self.diff_pages_section is not None and \
                self.diff_pages_section.has_page(pagenr):
            return True
        elif self.interdedupnointra_pages_section is not None and \
                self.interdedupnointra_pages_section.has_page(pagenr):
            return True
        elif self.interdedup_pages_section is not None and \
                self.interdedup_pages_section.has_page(pagenr):
            return True
        else:
            return False

    def did_page_change(self, pagenr) -> bool:
        return pagenr in self.changed_pages

    def _enumerate_changed_pages(self) -> set:

        if len(self.changed_pages) > 0:
            return self.changed_pages

        pagenr = 0
        num_pages = self.uncompressed_size / self.page_size
        self._changed_pages = set()
        while pagenr < num_pages:
            if self._did_page_change(pagenr):
                self._changed_pages.add(pagenr)
            pagenr += 1
        return self._changed_pages

    def _read_reference_name(self):
        cnt, self.reference_image_name = Util.read_unconstrained_null_terminated_string(self.src_fileobj)
        self.offset += cnt

    def _read_all_pages(self) -> bytes:
        fo = BytesIO(self.body_bytes)
        return fo.read()

    def _read_distinct_section(self, start: int):
        self.distinct_pages_section_start = start
        scn = MemscrimperDistinctSection(self.page_size, self.body_bytes, start)
        self.distinct_pages_section = scn
        try:
            scn.load()
        except:
            raise
        self.distinct_pages_section_end = scn.end
        consumed = scn.end - start
        return consumed

    def _read_diff_section(self, start: int):
        self.diff_pages_section_start = start
        scn = MemscrimperDiffSection(self.body_bytes, start)
        self.diff_pages_section = scn
        try:
            scn.load()
        except:
            raise
        self.diff_pages_section_end = scn.end
        consumed = scn.end - start
        return consumed

    def _read_interdedupnointra_section(self, start: int):
        self.interdedupnointra_pages_section_start = start
        scn = MemscrimperInterDedupSection(self.body_bytes, start)
        self.interdedupnointra_pages_section = scn
        try:
            scn.load()
        except:
            raise
        self.interdedupnointra_pages_section_end = scn.end
        consumed = scn.end - start
        return consumed

    def _read_interdedup_section(self, start: int):
        self.interdedup_pages_section_start = start
        scn = MemscrimperInterIntraDedupSection(self.page_size, self.body_bytes, start)
        self.interdedup_pages_section = scn
        try:
            scn.load()
        except:
            raise
        self.interdedup_pages_section_end = scn.end
        consumed = scn.end - start
        return consumed

    def recover_page(self, page_num=None, offset=None):
        if page_num is None and offset is None:
            return None
        if offset is not None:
            page_num = (offset % self.page_size) / self.page_size
        return self._resolve_memory(page_num=page_num)

    def check_distict_page(self, page_num) -> bytes:
        if self.distinct_pages_section is not None:
            return self.diff_pages_section.recover_page(page_num=page_num)
        return None

    def check_interdup_page(self, page_num) -> bytes:
        if self.interdedup_pages_section is not None:
            return self.interdedup_pages_section.recover_page(page_num=page_num)
        return None

    def check_interdedupnointra_page(self, page_num) -> bytes:
        if self.interdedupnointra_pages_section is not None:
            return self.interdedupnointra_pages_section.recover_page(page_num=page_num)
        return None

    def check_diff_page(self, page_num) -> bytes:
        if self.diff_pages_section is not None:
            return self.diff_pages_section.recover_page(page_num=page_num)
        return None

    def _set_page_data_base(self):
        self._enumerate_changed_pages()
        if self.interdedup_pages_section is not None:
            self.page_data_base = self.interdedup_pages_section.page_data_base
        elif self.interdedupnointra_pages_section.page_data_base is not None:
            self.page_data_base = self.interdedupnointra_pages_section.page_data_base

    def handle_interdedup(self) -> int:
        self.log("parsing distinct pages: {:08x}".format(self.offset))
        consumed = self._read_distinct_section(self.offset)
        self.offset += consumed
        self.log("parsing interdedup pages: {:08x}".format(self.offset))
        consumed = self._read_interdedup_section(self.offset)
        self.offset += consumed
        self._set_page_data_base()
        # raise Exception("Not verified working handle_interdedup")
        return consumed

    def handle_interdedupdelta(self) -> int:
        self.log("parsing distinct pages: {:08x}".format(self.offset))
        consumed = self._read_distinct_section(self.offset)
        self.offset += consumed
        self.log("parsing diff pages: {:08x}".format(self.offset))
        consumed = self._read_diff_section(self.offset)
        self.offset += consumed
        self.log("parsing interdedup pages: {:08x}".format(self.offset))
        consumed = self._read_interdedup_section(self.offset)
        self.offset += consumed
        self._set_page_data_base()
        return consumed

    def handle_interdedupnointra(self) -> int:
        self.log("parsing distinct pages: {:08x}".format(self.offset))
        consumed = self._read_distinct_section(self.offset)
        self.offset += consumed
        # self.log("parsing diff pages: {:08x}".format(self.offset))
        # consumed = self._read_diff_section(self.offset)
        # self.offset += consumed
        self.log("parsing interdedupnointra pages: {:08x}".format(self.offset))
        consumed = self._read_interdedupnointra_section(self.offset)
        self.offset += consumed
        self._set_page_data_base()
        raise Exception("Not working handle_interdedupnointra")
        return consumed

    def handle_interdedupnointradelta(self) -> int:
        self.log("parsing distinct pages: {:08x}".format(self.offset))
        consumed = self._read_distinct_section(self.offset)
        self.offset += consumed
        self.log("parsing diff pages: {:08x}".format(self.offset))
        consumed = self._read_diff_section(self.offset)
        self.offset += consumed
        self.log("parsing interdedupnointra pages: {:08x}".format(self.offset))
        consumed = self._read_interdedupnointra_section(self.offset)
        self.offset += consumed
        self._set_page_data_base()
        raise Exception("Not verified working handle_interdedupnointradelta")
        return consumed
