''' Command Line Interface to PostgreSQL Uploading '''

from pgreaper.io.csv_reader import sample_file
from pgreaper import PG_DEFAULTS
import pgreaper
import click

@click.command()
@click.option('--zip', 'zip', flag_value=True, default=False)
@click.option('--text', 'text', flag_value=True, default=False,
    help='Copy a TXT or other tab-separated file')
@click.option('--csv', 'csv', flag_value=True, default=False,
    help='Copy a CSV or other delimiter separated file')
@click.option('--preview', 'preview', flag_value=True, default=False,
    help='Preview how PGReaper will interpret a file instead of loading it')
@click.option('--delim', default=',', help='Delimiter')
@click.option('--header', default=0, help='Line number of the header row', show_default=True)
@click.option('--skiplines', default=0, help='Number of lines after the header to skip', show_default=True)
@click.option('--dbname', default=PG_DEFAULTS['dbname'], help='PostgreSQL database', show_default=True)
@click.option('--user', default=PG_DEFAULTS['user'], help='PostgreSQL user', show_default=True)
@click.option('--host', default=PG_DEFAULTS['host'], help='PostgreSQL host', show_default=True)
@click.option('--password', default=PG_DEFAULTS['password'], required=False, prompt=True, hide_input=True)
@click.argument('filename', nargs=-1)
def copy(filename, zip, text, csv, preview, delim, header, skiplines, dbname,
    user, host, password=None):
    ''' Main interface to Postgres uploading functions '''
    
    file = filename[0]
    
    try:
        zipped_file = filename[1]
    except IndexError:
        zipped_file = None
    
    if zip:
        if zipped_file:
            # Grab the specific file in the archive
            try:
                zipped_file = int(zipped_file)
            except ValueError:
                pass
                
            file = pgreaper.read_zip(file)[zipped_file]
        else:
            # Print contents
            print(pgreaper.read_zip(file))
    
    if preview:
        if text:
            delim = '\t'
        
        # TODO: Make a new function just for previewing files
        for i in sample_file(file=file, chunk_size=10, delimiter=delim,
            header=header, skiplines=skiplines):
            print(i)
            break
    else:
        if text or csv:
            if text:
                delim = '\t'
            copy_delim(file, zip, zipped_file, delim, header, skiplines,
                dbname, user, host, password)
            
def copy_delim(file, zip, zipped_file, delim, header, skiplines,
    dbname, user, host, password):
    # Use is not None because zipped_file = 0 is a legit non-null option
    if zip and (zipped_file is None):
        raise ValueError('Please specify which file in the ZIP archive to upload.')

    # Load the file
    pgreaper.copy_csv(file, delimiter=delim, header=header, skiplines=skiplines,
        dbname=dbname, user=user, host=host, password=password)
        
    # Print report
    print('Finished loading file')