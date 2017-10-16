# Compress persons.json with various algorithms

import shutil
import gzip
import bz2
import lzma

# gzip
with open('persons.json', mode='rb') as infile, \
     gzip.open('compressed/persons.json.gz', mode='wb') as outfile:
     shutil.copyfileobj(infile, outfile)
     
# bzip2
with open('persons.json', mode='rb') as infile, \
     bz2.open('compressed/persons.json.bz2', mode='wb') as outfile:
     shutil.copyfileobj(infile, outfile)
     
# lzma
with open('persons.json', mode='rb') as infile, \
     lzma.open('compressed/persons.json.lzma', mode='wb') as outfile:
     shutil.copyfileobj(infile, outfile)