### Memscrimper Python Library

This library provides a general mechanism for accessing memory images 
diff'ed and compressed using Memscrimper [1].  The library makes it 
possible to read (random access and on demand).  The library parses and 
loads the Memscrimper header and body. Once complete, users/developers 
can associate the reference memory image, and then recover the 
requisite memory.  

### Python Installation

```bash
python3 -m venv example_env
git checkout https://gitlab.com/geekweek-vi/2.2-group/memscrimper-parser
source activate example_env/bin/activate
cd memscrimper-parser
python3 setup.py install
pip3 install ipython
cd ..
ipython
```

#### Assuming data is in the following directories:
* __source memory dump (diff)__ :/data/memory_dumps/baseline-test-4.compress
* __actual memory dump (full)__ :/data/memory_dumps/baseline-test-4.raw
* __reference memory dump (base)__ :/data/memory_dumps/baseline-test-1.raw

### Basic Usage

``` python
from memscrimper_parser.interface import Memscrimper
from io import BytesIO
from hashlib import md5

src_fileobj=open('/data/memory_dumps/baseline-test-4.compress', 'rb')
src_filename = '/data/memory_dumps/baseline-test-4.compress'
ref_filename = '/data/memory_dumps/baseline-test-1.raw'
act_filename = '/data/memory_dumps/baseline-test-4.raw'
load = True
load_ref_data = True
# Load of memscrimper interface and load the header + body
ms = Memscrimper(src_fileobj=src_fileobj, ref_filename=ref_filename, load=load, load_ref_data=load_ref_data)

# recover specific pages from the dump
interested_pages = list(range(0, 64))
pages = []
for pagenr in interested_pages:
    pages.append(ms.read_page(pagenr))

# recover pages from the dump with actual differences (diffs not applied)
mod_pages = []
for pagenr in interested_pages:
    p = ms.read_meta_page_num(pagenr)
    if p is not None:
        mod_pages.append(p)

read_meta_page_num
# recover page data to a fileobj
ms.read_to_target(target_filename='/tmp/decompressed-test.raw')

target_buffer = BytesIO()
# recover page data to a fileobj
ms.read_to_target(target_fileobj=target_buffer)
print(target_buffer.tell())


# test to check and ensure correctness
ms.audit_decompression(act_target_fileobj=open(act_filename, 'rb'), return_buffer=True, page_num=0)

# returns the bytes reconstructed from the image, and checks the actual file for integrity 
bio, log = ms.audit_decompression(act_target_fileobj=open(act_filename, 'rb'), return_buffer=True)
bio.seek(0)
print (md5(bio.read()).hexdigest())
print (md5(open(act_filename, 'rb').read()).hexdigest())
```