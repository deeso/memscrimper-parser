#! /usr/bin/env python3

"""
Script to dump the raw bytes from all pages that changed in a diff. This could
be used to look for new strings that appear in the memory dump.
"""

import argparse
import os
import os.path

from memscrimper_parser.interface import Memscrimper

def process_file(base, diff, out):
    load = True
    load_ref_data = True

    with open(diff, 'rb') as fin:
        dst = os.path.join(out, os.path.basename(diff))+".pages"
        print(f"writing {diff} to {dst}")

        ms = Memscrimper(src_fileobj=fin, ref_filename=base, load=load, load_ref_data=load_ref_data)
        with open(dst, 'wb') as fout:
            for page_num in ms.changed_pages:
                page_bytes = ms.read_meta_page_num(page_num)
                fout.write(page_bytes)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("base", help="base memory file")
    parser.add_argument("diff", help="diff file")
    parser.add_argument("--out", help="output directory", default="out/")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)

    process_file(args.base, args.diff, args.out)
