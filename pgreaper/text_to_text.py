'''
.. currentmodule:: pgreaper

TXT, CSV to JSON
------------------
.. autofunction:: text_to_json
.. autofunction:: csv_to_json

TXT, CSV to HTML
-----------------
.. autofunction:: text_to_html
.. autofunction:: csv_to_html

TXT, CSV to Markdown
---------------------
'''

__all__ = ['text_to_html', 'text_to_csv', 'text_to_json', 'csv_to_html',
    'csv_to_json', 'csv_to_md']

from pgreaper.core import table_to_csv, table_to_json, \
    table_to_html, table_to_md, read_text, read_csv, sample_file, \
    read_text, read_csv
from pgreaper.io import read_json
from pgreaper.zip import ZipReader
        
def text_to_csv(file, out, delimiter='\t', **kwargs):
    for chunk in sample_file(file, delimiter=delimiter, **kwargs):
        table_to_csv(chunk['table'], out)
                
def text_to_json(file, out, delimiter='\t', **kwargs):
    table_to_json(read_text(file, delimiter='\t', **kwargs), out)
                
def csv_to_json(file, out, delimiter=',', **kwargs):
    table = read_csv(file, delimiter=delimiter, **kwargs)
    table_to_json(table, out)
        
def csv_to_md(file, out, delimiter=',', **kwargs):
    for chunk in sample_file(file, delimiter=delimiter, **kwargs):
        table_to_md(chunk['table'], out)
        
def text_to_html(file, out, delimiter='\t', **kwargs):
    table = read_csv(file, delimiter=delimiter, **kwargs)
    table_to_html(table, file=out)
        
def csv_to_html(file, out, delimiter=',', **kwargs):
    table = read_csv(file, delimiter=delimiter, **kwargs)
    table_to_html(table, file=out)