from io import BytesIO
from .consts import *
from .util import Util
import logging

mp_logger = logging.getLogger(__name__)

class MemscrimperSection(object):
    name = 'MemscrimperSection'
    DEBUG = False
    def __init__(self, body_bytes: bytes, start: int = 0, page_size: int = 4096):
        self.body_bytes = body_bytes

        self.start = start
        self.end = None
        self.pages_num = None

        self.pagenr_list_start = None
        self.pagenr_list_end = None
        self.pagenr_list = None

        self.interval_list_start = None
        self.interval_list_end = None
        self.interval_list = None

        self.patches_list_start = None
        self.patches_list_end = None
        self.patches_list = None

        self.reference_pages = {}
        self.page_data = {}
        self.page_size = page_size
        self.page_data_base = None

        self.ref_fileobj = None
        self.ref_name = None

    def associate_reference(self, ref_name, ref_fileobj=None, ref_offset=0):
        self.ref_name = ref_name
        self.ref_fileobj = ref_fileobj
        self.ref_offset = 0
        if self.ref_fileobj is None:
            try:
                self.ref_fileobj = open(ref_name, 'wb')
            except:
                pass
        self.ref_offset = ref_offset

    @classmethod
    def log(cls, msg, how='debug'):
        if cls.DEBUG:
            mp_logger.debug("{} {}".format(cls.name, msg))

    def build_references(self) -> dict:
        raise Exception("Not Implemented")

    def _parse_page_num(self):
        end = self.start + DWORD
        self.pages_num = Util.parse_dword(self.body_bytes[self.start:end])

    def _parse_pagenr_list(self):
        # if self.pagenr_list_start is None:
        #     self.pagenr_list_start = self.start + DWORD
        self.log("Parsing a pagenr_list @ 0x{:08x}".format(self.pagenr_list_start))
        start = self.pagenr_list_start
        consumed, pagenr_list = self.parse_pagenr_lists(start, self.body_bytes)
        self.log("Consumed {} a pagenr_list @ 0x{:08x}".format(len(pagenr_list), self.start))
        self.log("Recieved, consumed: {} pagenr_list len: {}".format(consumed, len(pagenr_list)))
        self.pagenr_list_end = self.pagenr_list_start + consumed
        self.pagenr_list = pagenr_list
        self.pages_num = len(pagenr_list)
        return consumed

    @classmethod
    def parse_pagenr_lists(cls, start: int, data, limit: int = 200000) -> (int, list):

        offset = start
        # fo = BytesIO(data)
        # self.num_pagenrs = Util.parse_dword(num_pagenrs_bytes)
        num_pagenrs = Util.parse_dword(data[offset:offset + DWORD])
        # self.log("Reading pagenr lists {}".format(num_pagenrs))
        pagenr_list = []
        if limit < num_pagenrs:
            raise Exception("Number of Page NRs exceeds limit: {}, got {}".format(limit, num_pagenrs))
        offset += DWORD
        # data = fo.read(DWORD*num_pagenrs)
        prev = None
        cnt = 0

        while cnt < num_pagenrs:
            # if cnt == stop:
            #     raise Exception("Stop here")
            a = Util.parse_byte(data[offset:offset + BYTE])
            # cls.log("Parsing pagenr ({}) list from {:08x} for {}".format(cnt, offset + 0x27, a))
            if a & 128 == 128:
                a &= 127
                offset += BYTE
            else:
                o, w = (1, 2)
                b = Util.parse_byte(data[offset + o:offset + w])
                o += 1
                w += 1
                c = Util.parse_byte(data[offset + o:offset + w])
                o += 1
                w += 1
                d = Util.parse_byte(data[offset + o:offset + w])
                a = (a << 24) | (b << 16) | (c << 8) | d
                # a = Util.parse_dword(data[offset:offset+DWORD], little_endian=True)
                offset += DWORD

            x = a
            if prev is None:
                prev = a
            else:
                x = prev + a + 1
                prev = prev + a + 1
            pagenr_list.append(x)
            # cls.log("Value = {} @ 0x{:08x}".format(x, offset))
            cnt += 1
        cls.log("Read the 0x%08x bytes, len(pagenr_list) = %d" % (offset, len(pagenr_list)))
        return offset - start, pagenr_list

    @classmethod
    def parse_interval_list(cls, start: int, data: bytes) -> (int, list):
        base = start
        interval_list = []
        cur_offset = start
        last = False
        cnt = 0
        # self.log("Parsing interval list @ 0x{:08x}".format(base))
        while not last:
            next_bytes = data[cur_offset:cur_offset + 2 * DWORD]
            if len(next_bytes) < 2 * DWORD:
                break
            results = cls.parse_interval_value(next_bytes)
            if results is None:
                raise Exception("Invalid Interval value returned @ 0x{:08x}".format(base + cur_offset))
            consumed, last, left, end = results
            # self.log("Parsed interval item {} @ 0x{:08x} last={} left={} right={}".format(cnt, cur_offset, last, left, end))
            # self.log("Parsed interval item @ 0x{:08x} consumed {}".format(cur_offset, consumed))
            interval_list.append([left, end])
            cur_offset += consumed
            cnt += 1
        # self.log("Completed Interval {}, consumed {} @ 0x{:08x}".format(cnt, cur_offset, cur_offset + base))
        return cur_offset - start, interval_list

    @classmethod
    def parse_interval_lists(cls, num_lists, start: int, data: bytes) -> (int, list):
        base = start
        cur_offset = start
        intervals_result = {}
        cnt = 0
        last_offset = cur_offset
        cls.log("Starting interval_list parsing for {}, @ 0x{:08x}".format(num_lists, cur_offset))
        while cnt < num_lists:
            # self.log("Parsing Interval {} @ 0x{:08x}".format(cnt, cur_offset))
            consumed, interval_list = cls.parse_interval_list(cur_offset, data)
            intervals_result[cnt] = interval_list
            cnt += 1
            cur_offset += consumed
            last_offset = cur_offset
        consumed = cur_offset - start
        cls.log("Completed Interval {}, consumed {} @ 0x{:08x}".format(cnt, consumed, cur_offset))
        return consumed, intervals_result

    @classmethod
    def parse_interval_value(cls, data: bytes) -> (bool, int, int):
        if len(data) < DWORD:
            raise Exception(
                "Parsing error: not enough data to parse interval value, expected 8-bytes, got {} bytes".format(
                    len(data)))
        left = Util.parse_dword(data[:DWORD])
        offset = DWORD
        upper = (left & (7 << 29)) >> 29
        sz = upper & 3
        last = (upper >> 2) == 1
        if sz == 3:
            sz = 4
        left &= (1 << 29) - 1
        if sz == 0:
            delta = 0
        elif sz == 1 and len(data) >= DWORD + BYTE:
            delta = Util.parse_byte(data[DWORD:DWORD + BYTE])
            offset += BYTE
        elif sz == 2 and len(data) >= DWORD + WORD:
            delta = Util.parse_word(data[DWORD:DWORD + WORD])
            offset += WORD
        elif sz == 4 and len(data) >= DWORD + DWORD:
            delta = Util.parse_dword(data[DWORD:DWORD + DWORD])
            offset += DWORD
        else:
            raise Exception("Parsing error: interval size %d is not in [1, 2, 4]", sz)
            # return None
        return offset, last, left, left + delta

    def _load_section(self):
        raise Exception("Nut implemented")

    def load(self):
        self._load_section()
        return self

    @classmethod
    def offset_to_page_num(cld, offset, page_size: int = 4096):
        page_addr = offset % page_size
        return page_addr / page_size

    def has_page(self, page_num=None, offset=None) -> bool:
        if page_num is not None:
            return page_num in self.reference_pages
        if offset is not None:
            page_num = self.offset_to_page_num(offset, self.page_size)
            return page_num in self.reference_pages

    # def recover_page(self, page_num=None, offset=None) -> bytes:
    #
    #     if offset is None and page_num is None:
    #         return None
    #     if page_num is None:
    #         page_num = self.offset_to_page_num(offset, self.page_size)
    #     if not self.has_page(page_num=page_num):
    #         return None
    #     return self._recover_page(page_num)

    def _recover_page(self, page_num):
        raise Exception("Not implemeneted")

    def associate_reference_file(self, filename=None, data_bytes=None, load_from_file=False):
        self.ref_filename = filename
        self.ref_bytes = data_bytes
        self.ref_loaded = load_from_file

        if self.ref_loaded and data_bytes is None:
            self.ref_bytes = open(self.ref_filename, 'rb').read()
            self.ref_fileobj = BytesIO(self.ref_bytes)
            self.ref_loaded_offset = 0

        elif self.ref_loaded:
            self.ref_fileobj = BytesIO(self.ref_bytes)
        else:
            self.ref_fileobj = open(self.ref_filename, 'rb')

    def read_page(self, page_num=None, offset=None, force_read=False):
        if offset is None and page_num is None:
            return None
        if page_num is None:
            page_num = self.offset_to_page_num(offset, self.page_size)

        if force_read or self.page_data.get(page_num, None) is None:
            self.ref_fileobj.seek(page_num * self.page_size)
            self.page_data[page_num] = self.ref_fileobj.read()
        return self.page_data[page_num]


class MemscrimperDistinctSection(MemscrimperSection):
    name = 'MemscrimperDistinctSection'

    def __init__(self, page_size: int, body_bytes: bytes, start: int = 0):
        super(MemscrimperDistinctSection, self).__init__(body_bytes, start)
        self.page_size = page_size

    def _load_section(self):
        # self._parse_page_num()
        self.pagenr_list_start = self.start  # + DWORD
        consumed = self._parse_pagenr_list()
        self.pagenr_list_end = consumed + self.pagenr_list_start
        self.log("Found {} pagenrs starting at {:08x}".format(len(self.pagenr_list), self.pagenr_list_start))
        self.log("Found {} pagenrs starting at {:08x}".format(len(self.pagenr_list), self.pagenr_list_end))
        self.interval_list_start = self.pagenr_list_end
        self.log("Parsing the interval_list from {:08x}".format(self.interval_list_start))
        self.pages_num = len(self.pagenr_list)

        consumed, interval_lists = self.parse_interval_lists(self.pages_num,
                                                             self.interval_list_start,
                                                             self.body_bytes)

        # consumed = self._parse_interval_list(num_pages=len(self.pagenr_list))
        self.interval_list = interval_lists
        self.interval_list_end = consumed + self.interval_list_start
        self.end = self.interval_list_end
        self.log("Completed parsing the interval_list @ {:08x}".format(self.interval_list_end))
        self.build_references()
        return self.end - self.start

    def build_references(self) -> dict:
        for i in range(len(self.pagenr_list)):
            pagenr_item = self.pagenr_list[i]
            for left, right in self.interval_list[i]:
                for pagenr in range(left, right + 1):
                    self.reference_pages[pagenr] = pagenr_item
        return self.reference_pages

    # def _recover_page(self, page_num, force_resolve=False):
    #     if self.ref_fileobj is not None:
    #         x = self.reference_pages[page_num]
    #         offset = x * self.page_size + self.ref_offset
    #         cur_offset = self.ref_fileobj.tell()
    #         self.ref_fileobj.seek(0)
    #         self.ref_fileobj.seek(offset)
    #         self.page_data[page_num] = self.ref_fileobj.read(self.page_size)
    #         self.ref_fileobj.seek(0)
    #         self.ref_fileobj.seek(cur_offset)
    #     # falls through to here, pass'em if you got'em
    #     return self.page_data.get(page_num, None)

    def convert_pagenr(self, pagenr):
        return self.reference_pages[pagenr]


class MemscrimperInterDedupSection(MemscrimperSection):
    name = 'MemscrimperInterDedupSection'

    def __init__(self, page_size: int, body_bytes: bytes, start: int = 0):
        super(MemscrimperInterDedupSection, self).__init__(body_bytes, start)
        self.page_size = page_size

    def _load_section(self):
        self.interval_list_start = self.start
        self.log("Parsing the interval_list from {:08x}".format(self.interval_list_start))
        consumed, interval_list = self.parse_interval_list(self.interval_list_start, self.body_bytes)
        self.interval_list = [interval_list]
        self.interval_list_end = consumed + self.interval_list_start
        self.log("Completed parsing the interval_list @ {:08x}".format(self.interval_list_end))

        # consumed = self._parse_interval_list(num_pages=len(self.pagenr_list))
        # self.interval_list_end = consumed + self.interval_list_start
        self.page_data_base = self.interval_list_end
        self.pages_num = len(self.interval_list[0])
        self.end = self.page_data_base + self.pages_num * self.page_size
        self.log("Start of page data @ {:08x}".format(self.page_data_base))
        self.log("End of page data @ {:08x}".format(self.end))
        self.log("Actual end of page data @ {:08x}".format(len(self.body_bytes)))
        self.build_references()
        return self.end - self.start

    def build_references(self) -> dict:
        file_offset = self.page_data_base
        src_page_num = 0
        cur_cnt = 0
        cnt = 0
        for i in range(len(self.interval_list)):
            self.log("{} Adding {} intervals page ref for @ {:08x}".format(i, len(self.interval_list[i]), file_offset))
            for left, right in self.interval_list[i]:
                self.log("Intervals page {} --> {} @ {:08x}".format(left, right + 1, file_offset))
                for j in range(left, right + 1):
                    self.reference_pages[j] = src_page_num
            self.log("Pages inserted {}/{}, iteration {} Adding page ref @ {:08x}".format(cur_cnt, cnt, i, file_offset))
            file_offset += self.page_size
            src_page_num += 1
        return self.reference_pages


    # def _recover_page(self, page_num, force_resolve=False):
    #     if page_num in self.page_data and not force_resolve:
    #         return self.page_data[page_num]
    #
    #     if self.ref_fileobj is not None:
    #         offset = page_num * self.page_size + self.ref_offset
    #         cur_offset = self.ref_fileobj.tell()
    #         self.ref_fileobj.seek(0)
    #         self.ref_fileobj.seek(offset)
    #         self.page_data[page_num] = self.ref_fileobj.read(self.page_size)
    #         self.ref_fileobj.seek(0)
    #         self.ref_fileobj.seek(cur_offset)
    #     # falls through to here, pass'em if you got'em
    #     return self.page_data.get(page_num, None)


class MemscrimperInterIntraDedupSection(MemscrimperSection):
    name = 'MemscrimperInterIntraDedupSection'

    def __init__(self, page_size: int, body_bytes: bytes, start: int = 0):
        super(MemscrimperInterIntraDedupSection, self).__init__(body_bytes, start)
        self.page_size = page_size
        self.pages = {}

    def _load_section(self):
        self.pages_num = Util.parse_dword(self.body_bytes[self.start:self.start + DWORD])
        self.interval_list_start = self.start + DWORD
        self.log("Parsing the interval_list from {:08x}".format(self.interval_list_start))
        consumed, interval_lists = self.parse_interval_lists(self.pages_num,
                                                             self.interval_list_start,
                                                             self.body_bytes)
        self.interval_list = interval_lists
        self.interval_list_end = self.interval_list_start + consumed
        self.page_data_base = self.interval_list_end
        self.end = self.page_size * self.pages_num
        self.build_references()
        return self.end - self.start

    def build_references(self):
        file_offset = self.page_data_base
        src_page_num = 0
        cnt = 0
        for i in range(len(self.interval_list)):
            cur_cnt = 0
            interval_list = self.interval_list[i]
            self.log("{} Adding {} intervals page ref for @ {:08x}".format(i, len(interval_list), file_offset))
            for left, right in interval_list:
                self.log("Intervals page {} --> {} @ {:08x}".format(left, right + 1, file_offset))
                for j in range(left, right + 1):
                    cnt += 1
                    cur_cnt += 1
                    self.reference_pages[j] = src_page_num
            self.log("Pages inserted {}/{}, iteration {} Adding page ref @ {:08x}".format(cur_cnt, cnt, i, file_offset))
            file_offset += self.page_size
            src_page_num += 1

    # def _recover_page(self, page_num, force_resolve=False):
    #     if page_num in self.page_data and not force_resolve:
    #         return self.page_data[page_num]
    #     page_offset = self.reference_pages[page_num]
    #     page_data = self.body_bytes[page_offset:page_offset + self.page_size]
    #     self.page_data[page_num] = page_data
    #     return page_data

    def convert_pagenr(self, pagenr):
        return self.reference_pages[pagenr]



class MemscrimperDiffSection(MemscrimperSection):
    name = 'MemscrimperDiffSection'

    def __init__(self, body_bytes: bytes, start: int = 0):
        super(MemscrimperDiffSection, self).__init__(body_bytes, start)
        self.diff_pages = None
        self.num_patches = None

    def build_references(self) -> dict:
        for i in range(len(self.pagenr_list)):
            self.reference_pages[self.pagenr_list[i]] = self.patches_list[i]
        return self.reference_pages

    def _load_section(self):
        # self._parse_diff()
        self.pagenr_list_start = self.start  # + DWORD
        consumed = self._parse_pagenr_list()
        end = consumed + self.pagenr_list_start
        self.log("Found {} pagenrs starting at {:08x}".format(len(self.pagenr_list), self.pagenr_list_start))
        self.log("Completed pagenrs at {:08x}".format(self.pagenr_list_end))
        self.patches_list_start = self.pagenr_list_end
        self.log("Parsing the diffs from {:08x}".format(self.patches_list_start))
        consumed = self._parse_diff(num_pages=len(self.pagenr_list))
        self.log("Found {} diffs from {:08x}".format(len(self.patches_list), self.patches_list_start))
        self.log("Completed diff read at {:08x}".format(self.patches_list_end))
        self.end = self.patches_list_end
        self.build_references()
        return self.end - self.start

    def _parse_diff(self, num_pages):
        self.num_patches = num_pages
        if self.patches_list_start is None:
            self.patches_list_start = self.start

        cur_offset = self.patches_list_start
        self.patches_list = []
        self.diff_pages = {}
        cnt = 0
        stop = 3
        pd_start = cur_offset
        self.log("Processing diff {} at {:08x}".format(self.num_patches, cur_offset))
        while cnt < self.num_patches:
            # if cnt == stop:
            #     raise
            # self.log("Processing diff {} at {:08x}".format(cnt, cur_offset))
            consumed, diff_interval = self.parse_diff_intreval(self.body_bytes, cur_offset)
            self.patches_list.append(diff_interval)
            # self.log("Processed diff {} at {:08x}, consumed: {}".format(cnt, cur_offset, consumed))
            cur_offset += consumed
            cnt += 1
        consumed = cur_offset - pd_start
        self.log("Processed diff {} at {:08x}, consumed: {}".format(cnt, cur_offset, consumed))
        self.patches_list_end = cur_offset
        return consumed

    def parse_diff_intreval(self, data, cur_offset: int = 0) -> (int, list):
        ret = []
        num_diffs = Util.parse_word(data[cur_offset:cur_offset + DWORD])
        cnt = 0
        di_start = cur_offset
        cur_offset += WORD
        # self.log("Parsing {} diff items @ 0x{:08x}".format(num_diffs, cur_offset))

        while cnt < num_diffs:
            self.log("Parsing diff item {} @ 0x{:08x}".format(cnt, cur_offset))
            # self.log("Processing diff value {} at {:08x}".format(cnt, cur_offset))
            new_bytes = data[cur_offset:cur_offset + 3]
            consumed, rel, sz = Util.decode(new_bytes)
            start = cur_offset + consumed
            end = cur_offset + sz + consumed + 1
            cur_offset = end
            # self.log("Consumed bytes {} (sz: {}, rel:{}) @ 0x{:08x}".format(consumed+sz, sz, rel, cur_offset))
            # self.log("Processing diff value {}, consumed {} at {:08x}".format(cnt, (consumed+sz), cur_offset))
            diff_data = data[start:end]
            ret.append([rel, diff_data])
            cnt += 1

        # self.log("Completed processing diff {} values at {:08x}, consumed: {}".format(cnt, cur_offset, cur_offset-di_start))
        return cur_offset - di_start, ret

    def build_references(self) -> dict:
        for i in range(len(self.pagenr_list)):
            pagenr_item = self.pagenr_list[i]
            self.reference_pages[pagenr_item] = self.patches_list[i]
        return self.reference_pages

    @classmethod
    def apply_diffs(cls, page_data, diffs) -> bytes:
        ret = bytearray(page_data)
        offset = 0
        for (rel, bs) in diffs:
            offset += rel
            for i in range(len(bs)):
                ret[offset + i] = bs[i]
            offset += len(bs)
        return bytes(ret)

    def apply_page_diff(self, page_num, page_data) -> bytes:
        if page_num in self.reference_pages:
            return self.apply_diffs(page_data, self.reference_pages[page_num])
        return None

    # def _recover_page(self, page_num, force_resolve=False):
    #     offset = self.page_size * page_num
    #     if page_num in self.page_data and not force_resolve:
    #         return self.page_data[page_num]
    #
    #     if self.ref_fileobj is not None:
    #         self.ref_fileobj.seek(offset)
    #         data = self.ref_fileobj.read(self.page_size)
    #         new_data = self.apply_diff(page_num, new_data)
    #         self.page_data[page_num] = new_data
    #         return new_data
    #     return None
    #     # raise Exception("Not implemeneted")

    def convert_pagenr(self, pagenr):
        return pagenr

