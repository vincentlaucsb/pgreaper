'''
.. currentmodule:: sqlify

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

from sqlify._globals import Singleton
from sqlify.core import chunk_file, table_to_csv, table_to_json, \
    table_to_html, table_to_md, \
    text_to_table, csv_to_table, read_json
from sqlify.zip import ZipReader

class TextTransformer(metaclass=Singleton):
    '''
    Capable of converting from any one file type to another as long as:
     * SQLify can read it into a Table
     * The Table has a method to output it to the desired format
     
    Not meant to be used directly by end-users
    '''
    
    # Mappings of file types to reading functions
    src = {
        'txt': chunk_file,
        'csv': chunk_file
    }
    
    # Use the file chunker when read from these...
    # otherwise load all into memory
    use_chunker = ['txt', 'csv']
    
    # Mappings of file types to output functions
    dest = {
        'csv': table_to_csv,
        'json': table_to_json,
        'html': table_to_html,
        'md': table_to_md
    }
    
    @classmethod
    def convert(cls, src, dest, **kwargs):
        if not (src in cls.src and dest in cls.dest):
            raise NotImplementedError
        else:
            if src in cls.use_chunker:
                cls.convert_chunk(src, dest, **kwargs)
            else:
                cls.convert_no_chunk(src, dest, **kwargs)
            
    @classmethod
    def convert_no_chunk(cls, src_type, dest_type, infile, outfile,
        delimiter, **kwargs):
        ''' Convert a file by reading it all into memory at once '''
        pass
        
    @classmethod
    def convert_chunk(cls, src_type, dest_type, infile, outfile, delimiter,
        **kwargs):
        '''
        Convert a file by reading and writing it using chunks
        
        Parameters
        ----------
        infile
                Name of the source file
        outfile
                Name of the destination file
        '''

        for table in chunk_file(file=infile, delimiter=delimiter, **kwargs):
            cls.dest[dest_type](table, outfile)

def text_to_html(file, out, delimiter='\t', **kwargs):   
    TextTransformer.convert('txt', 'html', infile=file, outfile=out,
        delimiter=delimiter)
        
def text_to_csv(file, out, delimiter='\t', **kwargs):
    TextTransformer.convert('txt', 'csv', infile=file, outfile=out,
        delimiter=delimiter)
                
def text_to_json(file, out, delimiter='\t', **kwargs):
    TextTransformer.convert('txt', 'json', infile=file, outfile=out,
        delimiter=delimiter)
                
def csv_to_json(file, out, delimiter=',', **kwargs):
    '''
    Convert a CSV file to JSON
    
    Parameters
    ----------
    x : type
        Description of parameter `x`.
    y
        Description of parameter `y` (with type not specified)
    
    '''
    
    TextTransformer.convert('csv', 'json', infile=file, outfile=out,
        delimiter=delimiter)
        
def csv_to_md(file, out, delimiter=',', **kwargs):
    TextTransformer.convert('csv', 'md', infile=file, outfile=out,
        delimiter=delimiter)
        
def csv_to_html(file, out, delimiter=',', **kwargs):
    TextTransformer.convert('csv', 'html', infile=file, outfile=out,
        delimiter=delimiter)