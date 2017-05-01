'''

Walks user through creating an SQL database

==== TO DO ====
Give user ability to review what table will look like after choosing a delim

'''

from sqlify.cli.helpers import _hrule, _to_pretty_table, _validate_response
from sqlify.helpers import _strip

from .. import sqlify
from .. import utils
from .. import settings

import click
import os

CURRENT_PATH = os.getcwd()

# Horizontal line
H_RULE = '='*75

# To call, type `sqlify` from the command line
@click.command()
def cli():
    click.echo('Welcome to SQLify Version 1.0.1')
    click.echo('')
    
    click.echo(H_RULE)
    click.echo('PostgresSQL Configuration Settings (from settings.py)')
    click.echo('Default User: ' + settings.POSTGRES_DEFAULT_USER)
    click.echo('Default Database: ' + settings.POSTGRES_DEFAULT_DATABASE)
    click.echo('Default Host: ' + settings.POSTGRES_DEFAULT_HOST)
    click.echo('Default Password: ' + '*'*len(settings.POSTGRES_DEFAULT_PASSWORD))
    click.echo(H_RULE)
    click.echo('')
    
    click.echo('[1] Read the first few lines of a file')
    click.echo('[2] Convert a file to SQL')
    click.echo('...')
    click.echo('[0] Quit')
    click.echo('')
    
    while True:
        response = click.prompt('Please enter an number to continue')
    
        if response.isnumeric():
            if response == '1':
                preview()
            elif response == '2':
                to_sql()
                
            break
        else:
            click.echo('Invalid response. Enter "0" or Ctrl + C to quit.')
            click.echo('\n')
            
def to_sql(filename=None):
    # Ask for filename
    if not filename:
        filename = click.prompt(
            'Please enter a file (relative to {0})'.format(CURRENT_PATH),
            type=click.Path(exists=True))

    # Get basic file information
    file_info = head(filename)
    
    click.echo(file_info['tbl'])
    delimiter = file_info['delimiter']
    
    # Get primary key
    p_key_colname = get_primary_key(file_info['tbl'])
    
    if p_key_colname == '0':
        p_key = None
    else:
        # Return index of column
        p_key = file_info.col_names.index(p_key_colname)

    # Database options
    engines = {
            '1': 'sqlite',
            '2': 'postgres'
        }
    
    engine_choice = engines[_validate_response(
        prompt = 'Enter a number',
        text = [
            'Which database engine would you like to use?',
            '[1] SQLite',
            '[2] PostgresSQL'
        ],
        valid = engines.keys(),
        default='1'
    )]
    
    database = click.prompt('Enter a database to save to')
    table_name = click.prompt('What do you want to name your table (Enter nothing to use default (in brackets))',
        default=_strip(filename.split('.')[0]))
    
    # Use text_to_sql or csv_to_sql depending on the file extension
    file_ext = filename[-3:]
    
    if 'txt' == file_ext:
        func = sqlify.text_to_sql
    else:
        func = sqlify.csv_to_sql
    
    if engine_choice == 'postgres':
        pg_settings = postgres_settings()
    else:
        pg_settings = {}
    
    # Create the database            
    database_full_path = os.path.join(CURRENT_PATH, database)
    
    func(file=filename,
        name=table_name,
        database=database_full_path,
        delimiter=delimiter,
        engine=engine_choice,
        p_key=p_key,
        **pg_settings)
    
    click.echo('Created SQL database')
    
def preview():
    filename = click.prompt('What is the filename?',
        type=click.Path(exists=True))
    
    # Give a short preview
    for i in utils.preview(filename, n=10):
        click.echo(i)
    
    # Ask for follow-up
    option = click.prompt('Would you like to convert this file to SQL? [y/n]')
    
    if option.lower() == 'y':
        to_sql(filename)
    else:
        quit()
        
# Get basic file information
def head(filename):
    delimiter = click.prompt('How is the data delimited (separated)?')

    # Text or CSV/DSV?
    file_choice = _validate_response(
        prompt='Enter a number',
        valid=[1, 2],
        text=[
            'What type of data file is this?',
            '[1] Text file',
            '[2] CSV/TSV/DSV (comma-separated ' + 
                'values/tab-sep.../delimiter-sep/etc..)'
        ]
    )
        
    if file_choice == '1':
        file_type = 'text'
    else:
        file_type = 'csv'
            
    tbl = utils.head(filename, type=file_type, delimiter=delimiter)
    
    return {'tbl': tbl, 'delimiter': delimiter}

def get_primary_key(tbl):
    '''
    Prompt user about which column is the primary key

    Arguments:
     * tbl:     A Table object with basic info about the file, i.e.
                column names, first few rows
    '''
    
    col_names = tbl.col_names
        
    return _validate_response(
        prompt='Which column do you want as the primary key? \n' + 
               "(Enter 0 if you don't know/want/have one)",
        text=[_hrule('Begin column names'),
              _to_pretty_table(col_names),
              _hrule('End column names')
             ],
        valid=col_names + [0]
    )
    
# Prompt user for input on connecting to a Postgres database
def postgres_settings():
    username = click.prompt(
        "What username do you want to connect with?\n" +
        "(Leave blank to use {0})".format(settings.POSTGRES_DEFAULT_USER),
        default=settings.POSTGRES_DEFAULT_USER)
        
    password = click.prompt(
        "What password do you want to connect with?\n" +
        "(Leave blank to use default)",
        hide_input=True, confirmation_prompt=True,
        default=settings.POSTGRES_DEFAULT_PASSWORD)
        
    return {'username': username, 'password': password}